import random
import json

def create_gangs(num_gangs):
    gangs_data = []
    for i in range(num_gangs):
        num_tasks = random.randint(1, 3)  # Each gang has 1 to 3 tasks
        inter_arrival_time = random.randint(1, 10)  # Uniform whole number inter-arrival times between 1 and 10

        # Generate bursts for each task in the gang
        tasks_bursts = []
        for _ in range(num_tasks):
            bursts = []
            burst_type = 'CPU'
            for _ in range(random.randint(1, 5)):
                bursts.append((burst_type, random.randint(3, 20)))
                burst_type = 'IO' if burst_type == 'CPU' else 'CPU'
            tasks_bursts.append(bursts)
        
        gangs_data.append({
            'gang_id': i + 1,
            'inter_arrival_time': inter_arrival_time,
            'num_tasks': num_tasks,
            'tasks_bursts': tasks_bursts
        })
    return gangs_data

def save_gangs_to_file(gangs_data, filename):
    with open(filename, 'w') as f:
        json.dump(gangs_data, f, indent=4)

# Create gangs and save to a file
gangs_data = create_gangs(4)  # Change the number of gangs as needed
save_gangs_to_file(gangs_data, 'gangs_data.json')
print("Gangs data created and saved to gangs_data.json")
