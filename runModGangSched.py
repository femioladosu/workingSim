

import simpy
import random
import json

# Define task states
STATE_READY = "READY"
STATE_RUNNING = "RUNNING"
STATE_WAITING = "WAITING"
STATE_FINISHED = "FINISHED"
STATE_TERMINATED = "TERMINATED"

# Define the time slice for round-robin scheduling
TIME_SLICE = 5

# Global variables to track the number of completed gangs and IO without processor
completed_gangs = 0
io_without_processor = 0
io_without_processor_tasks = {}

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
        global completed_gangs, io_without_processor, io_without_processor_tasks
        state = STATE_READY
        while bursts:
            burst_type, duration = bursts[0]
            if burst_type == 'CPU':
                time_slice = min(TIME_SLICE, duration)
                state = STATE_RUNNING
                print(f'{self.env.now}: {name} state: {state} - requesting CPU for {time_slice} time units')
                with self.cpu.request() as req:
                    yield req
                    print(f'{self.env.now}: {name} state: {state} - got CPU')
                    yield self.env.timeout(time_slice)
                    duration -= time_slice
                    if duration > 0 and len(bursts) > 0:
                        bursts[0] = (burst_type, duration)
                        state = STATE_READY
                        print(f'{self.env.now}: {name} state: {state} - preempted with {duration} time units remaining')
                        self.ready_queue.put((name, bursts, gang_id))  # Put back to ready queue if not finished
                    elif len(bursts) > 0:
                        bursts.pop(0)
                        state = STATE_WAITING
                        print(f'{self.env.now}: {name} state: {state} - finished CPU burst')
            else:
                state = STATE_WAITING
                print(f'{self.env.now}: {name} state: {state} - performing I/O for {duration} time units')
                if duration > TIME_SLICE:
                    io_without_processor += duration
                    if name not in io_without_processor_tasks:
                        io_without_processor_tasks[name] = 0
                    io_without_processor_tasks[name] += duration
                    duration -= TIME_SLICE
                    bursts[0] = (burst_type, duration)
                    print(f'{self.env.now}: {name} state: {state} - I/O burst longer than time slice, deallocating processor')
                    yield self.env.timeout(TIME_SLICE)
                else:
                    yield self.env.timeout(duration)
                    if len(bursts) > 0:
                        bursts.pop(0)
                        state = STATE_READY
                        print(f'{self.env.now}: {name} state: {state} - finished I/O burst')

        state = STATE_FINISHED
        print(f'{self.env.now}: {name} state: {state} - task completed')
        if gang_id in self.gangs:
            if name in self.gangs[gang_id]:
                self.gangs[gang_id].remove(name)
                if not self.gangs[gang_id]:  # All tasks in the gang are finished
                    print(f'{self.env.now}: Gang {gang_id} state: {STATE_TERMINATED} - all tasks completed')
                    del self.gangs[gang_id]  # Remove the gang
                    completed_gangs += 1  # Increment the global counter for completed gangs
            else:
                print(f'{self.env.now}: Error - {name} not found in gang {gang_id} tasks')
        else:
            print(f'{self.env.now}: Error - Gang {gang_id} not found')

    def create_gang(self, gang_id, inter_arrival_time, tasks_bursts):
        """Function to create and process a gang of tasks."""
        yield self.env.timeout(inter_arrival_time)  # Simulate the arrival of the gang
        print(f"Gang {gang_id} arrived at {self.env.now}")
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
    global completed_gangs
    # Wait for all gangs to be created
    yield simpy.events.AllOf(env, gang_creation_processes)
    
    total_gangs = len(scheduler.gangs)
    print(f'Total gangs created: {total_gangs}')
    
    while completed_gangs < total_gangs:  # Loop until all gangs are completed
        if scheduler.ready_queue.items:
            task_name, bursts, gang_id = yield scheduler.ready_queue.get()
            print(f'{env.now}: Scheduling {task_name}')
            env.process(scheduler.task(task_name, bursts, gang_id))

        print(f'{env.now}: Completed gangs: {completed_gangs}/{total_gangs}')
        yield env.timeout(1)  # Time tick

# Create a SimPy environment
env = simpy.Environment()
# Setup the environment with 4 gangs and 6 CPU cores
setup_environment(env, 4)
# Run the simulation
env.run()

print(f'Total IO done without processor: {io_without_processor} time units')
print('Individual IO without processor:')
for task, io_time in io_without_processor_tasks.items():
    print(f'{task}: {io_time} time units')





