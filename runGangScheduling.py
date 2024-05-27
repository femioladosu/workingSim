import simpy
import json

# Define task states
STATE_READY = "READY"
STATE_RUNNING = "RUNNING"
STATE_WAITING = "WAITING"
STATE_FINISHED = "FINISHED"
STATE_TERMINATED = "TERMINATED"

# Define the time slice for round-robin scheduling
TIME_SLICE = 5

# Global variables to track metrics
completed_gangs = 0
total_context_switches = 0
cpu_idle_time = 0
cpu_utilization_time = 0
task_wait_times = {}
task_io_times = {}
task_ready_queue_times = {}
gang_throughput_times = {}
turnaround_times = {}
response_times = {}
wait_times = {}
throughput_counts = []

class GangScheduler:
    def __init__(self, env, cpu_capacity):
        self.env = env
        self.cpu_capacity = cpu_capacity
        self.cpu = simpy.Resource(env, capacity=cpu_capacity)
        self.ready_queue = simpy.Store(env)  # Queue for ready tasks
        self.gangs = {}  # Track gangs and their tasks
        self.tasks = {}  # Track all tasks and their states

    def task(self, name, bursts, gang_id):
        """A task process that performs CPU and I/O operations with state management."""
        global completed_gangs, total_context_switches, cpu_utilization_time

        state = STATE_READY
        task_start_time = self.env.now
        task_io_time = 0
        task_ready_time = 0
        context_switches = 0
        
        while bursts:
            burst_type, duration = bursts[0]
            if burst_type == 'CPU':
                time_slice = min(TIME_SLICE, duration)
                state = STATE_RUNNING
                task_ready_queue_start_time = self.env.now
                print(f'{self.env.now}: {name} state: {state} - requesting CPU for {time_slice} time units')
                with self.cpu.request() as req:
                    yield req
                    task_ready_time += self.env.now - task_ready_queue_start_time
                    if name not in response_times:
                        response_times[name] = self.env.now - task_start_time  # Record response time
                    print(f'{self.env.now}: {name} state: {state} - got CPU')
                    cpu_utilization_time += time_slice
                    yield self.env.timeout(time_slice)
                    duration -= time_slice
                    if duration > 0 and len(bursts) > 0:
                        bursts[0] = (burst_type, duration)
                        state = STATE_READY
                        print(f'{self.env.now}: {name} state: {state} - preempted with {duration} time units remaining')
                        self.ready_queue.put((name, bursts, gang_id))  # Put back to ready queue if not finished
                        context_switches += 1  # Increment context switches for the task
                        total_context_switches += 1  # Increment global context switch counter
                    elif len(bursts) > 0:
                        bursts.pop(0)
                        state = STATE_WAITING
                        print(f'{self.env.now}: {name} state: {state} - finished CPU burst')
            else:
                state = STATE_WAITING
                print(f'{self.env.now}: {name} state: {state} - performing I/O for {duration} time units')
                yield self.env.timeout(duration)
                task_io_time += duration  # Track total I/O time for the task
                if len(bursts) > 0:
                    bursts.pop(0)
                    state = STATE_READY
                    print(f'{self.env.now}: {name} state: {state} - finished I/O burst')

        state = STATE_FINISHED
        print(f'{self.env.now}: {name} state: {state} - task completed')
        task_end_time = self.env.now
        turnaround_times[name] = task_end_time - task_start_time  # Record turnaround time
        if gang_id in self.gangs:
            if name in self.gangs[gang_id]:
                self.gangs[gang_id].remove(name)
                if not self.gangs[gang_id]:  # All tasks in the gang are finished
                    gang_end_time = self.env.now
                    gang_throughput_times[gang_id] = gang_end_time - gang_creation_times[gang_id]
                    throughput_counts.append(gang_id)  # Count throughput
                    print(f'{self.env.now}: Gang {gang_id} state: {STATE_TERMINATED} - all tasks completed')
                    del self.gangs[gang_id]  # Remove the gang
                    completed_gangs += 1  # Increment the global counter for completed gangs
            else:
                print(f'{self.env.now}: Error - {name} not found in gang {gang_id} tasks')
        else:
            print(f'{self.env.now}: Error - Gang {gang_id} not found')
        
        task_io_times[name] = task_io_time  # Update task I/O time
        task_ready_queue_times[name] = task_ready_time  # Update task ready queue time
        wait_times[name] = task_ready_time  # Update wait time

    def create_gang(self, gang_id, inter_arrival_time, tasks_bursts):
        """Function to create and process a gang of tasks."""
        yield self.env.timeout(inter_arrival_time)  # Simulate the arrival of the gang
        print(f"Gang {gang_id} arrived at {self.env.now}")
        gang_creation_times[gang_id] = self.env.now
        print(f"Gang {gang_id} processes and their bursts:")

        self.gangs[gang_id] = []
        for task_id, bursts in enumerate(tasks_bursts, start=1):
            task_name = f"Gang{gang_id}-Task{task_id}"
            self.gangs[gang_id].append(task_name)
            print(f"  {task_name}: {bursts}")
            self.ready_queue.put((task_name, bursts, gang_id))  # Add task to ready queue
            self.tasks[task_name] = bursts  # Track task

        print(f"Gang {gang_id}")

def setup_environment(env, cpu_capacity):
    """Setup and run the simulation environment."""
    scheduler = GangScheduler(env, cpu_capacity)
    gang_creation_processes = []

    # Load gang data from JSON file
    with open('gangs_data.json', 'r') as file:
        gangs_data = json.load(file)

    for gang in gangs_data:
        gang_id = gang['gang_id']
        inter_arrival_time = gang['inter_arrival_time']
        tasks_bursts = gang['tasks_bursts']

        # Print the details of each gang
        print(f"Loading Gang {gang_id} with inter-arrival time {inter_arrival_time} and task bursts: {tasks_bursts}")

        gang_creation_processes.append(env.process(scheduler.create_gang(gang_id, inter_arrival_time, tasks_bursts)))

    env.process(time_tick_scheduler(env, scheduler, gang_creation_processes))



def time_tick_scheduler(env, scheduler, gang_creation_processes):
    """Scheduler to handle task execution at each time tick."""
    global completed_gangs, cpu_idle_time
    # Wait for all gangs to be created
    yield simpy.events.AllOf(env, gang_creation_processes)
    
    total_gangs = len(scheduler.gangs)
    print(f'Total gangs created: {total_gangs}')
    
    while completed_gangs < total_gangs:  # Loop until all gangs are completed
        if scheduler.ready_queue.items:
            task_name, bursts, gang_id = yield scheduler.ready_queue.get()
            print(f'{env.now}: Scheduling {task_name}')
            env.process(scheduler.task(task_name, bursts, gang_id))
        else:
            cpu_idle_time += 1  # Increment CPU idle time when no tasks are in the ready queue

        print(f'{env.now}: Completed gangs: {completed_gangs}/{total_gangs}')
        yield env.timeout(1)  # Time tick

# Read gangs data from file and run the simulation
with open('gangs_data.json', 'r') as f:
    gangs_data = json.load(f)

# Create a SimPy environment
env = simpy.Environment()
gang_creation_times = {}  # Track gang creation times
# Setup the environment with the loaded gangs data and 6 CPU cores
setup_environment(env, 6)
# Run the simulation
env.run()

# Output the collected metrics
print("\nCollected Metrics:")
print(f"Total context switches: {total_context_switches}")
print(f"CPU idle time: {cpu_idle_time}")
print(f"CPU utilization time: {cpu_utilization_time}")

print("\nTask Wait Times:")
for task_name, wait_time in task_wait_times.items():
    print(f"  {task_name}: {wait_time}")

print("\nTask I/O Times:")
for task_name, io_time in task_io_times.items():
    print(f"  {task_name}: {io_time}")

print("\nTask Ready Queue Times:")
for task_name, ready_queue_time in task_ready_queue_times.items():
    print(f"  {task_name}: {ready_queue_time}")

print("\nGang Throughput Times:")
for gang_id, throughput_time in gang_throughput_times.items():
    print(f"  Gang {gang_id}: {throughput_time}")

print("\nTask Turnaround Times:")
for task_name, turnaround_time in turnaround_times.items():
    print(f"  {task_name}: {turnaround_time}")

print("\nTask Response Times:")
for task_name, response_time in response_times.items():
    print(f"  {task_name}: {response_time}")

print("\nTask Wait Times:")
for task_name, wait_time in wait_times.items():
    print(f"  {task_name}: {wait_time}")

print(f"\nThroughput Counts: {len(throughput_counts)}")
