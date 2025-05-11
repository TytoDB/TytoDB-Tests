from tytodb_client import ConnectionHandler
import random, secrets, base64, subprocess
from time import time  
from command_generator import CommandGenerator, gen_str


duration = 100 * 3600  
operations = 100

def get_command_name(id : int) -> str:
    if id == 1:
        return "Create row"
    elif id == 2:
        return "Delete row"
    elif id == 3:
        return "Edit row"
    elif id == 4:
        return "Query"
    elif id == 5:
        return "Next Query Page"
    elif id == 6:
        return "Previous Query Page"

def push_log(start, cmd, cycle, csv_path, error_message = ""):
    elapsed = time() - start
    line = f"{cycle},{get_command_name(cmd)},{elapsed:.4f},{start:.4f},{time():.4f},{error_message}\n"
    with open(csv_path, "a") as f:
        f.write(line)

def main():
    connection = ConnectionHandler("tytodb://127.0.0.1:5000:8000", base64.b64decode("FIgQg8Pe/X0p+zJKsE1eG4lnxiRPYtyUqQ6aXZ4UdW0="), 60)
    cycles = 0
    csv = f"test-{gen_str(5)}.csv"
    start = time()

    with open(f"./{csv}", "w") as f:
        f.write("Cycle,Command,Duration,Start time,End time,Error message\n") 

    while time() < start + duration and cycles < 1_000_000:
        try:
            container_result = CommandGenerator.CreateContainer()
            container_command = container_result[0]
            container_name = container_result[1][0]
            container_columns = container_result[1][1]
            container_types = container_result[1][2]
            connection.command(container_command, [])
            cycles += 1
            print(f"Starting cycle {cycles} with {operations} operations on container '{container_name}'")
            
            for index in range(operations):
                cmd = random.randint(1, 4)
                start_op = time()
                
                try:
                    if cmd == 1:
                        row_command = CommandGenerator.CreateRow(container_name, container_columns, container_types)
                        connection.command(row_command, [])
                        push_log(start_op, cmd, cycles, csv)
                    elif cmd == 2:
                        delete_command = CommandGenerator.DeleteRow(container_name, container_columns, container_types)
                        connection.command(delete_command, [])
                        push_log(start_op, cmd, cycles, csv)
                    elif cmd == 3:
                        edit_command = CommandGenerator.EditRow(container_name, container_columns, container_types)
                        connection.command(edit_command, [])
                        push_log(start_op, cmd, cycles, csv)
                    else:
                        search_command = CommandGenerator.SearchRow(container_name, container_columns, container_types)
                        query = connection.command(search_command, [])
                        push_log(start_op, cmd, cycles, csv)
                        
                        if hasattr(query, 'pages') and query.pages:
                            page_count = len(query.pages)
                            for j in range(min(page_count, 10)): 
                                start_page = time()
                                try:
                                    query.next_page()
                                    push_log(start_page, 5, cycles, csv)
                                except Exception as e:
                                    print(f"Error navigating to next page: {str(e)}")
                            for j in range(min(page_count, 10)): 
                                start_page = time()
                                try:
                                    query.previous_page()
                                    push_log(start_page, 6, cycles, csv)
                                except Exception as e:
                                    print(f"Error navigating to previous page: {str(e)}")
                
                except Exception as e:
                    print(f"Error executing operation {get_command_name(cmd)}: {str(e)}")
                    push_log(start,cmd,cycles,csv,str(e))
                
                
                if index % 100 == 0:
                    print(f"Cycle {cycles}, operation {index}/{operations} completed")
            
            print(f"Cycle {cycles} finished successfully, now starting cycle {cycles+1}.")
            try:
                delete_container_command = CommandGenerator.DeleteContainer(container_name)
                connection.command(delete_container_command, [])
            except Exception as e:
                print(f"Error deleting container '{container_name}': {str(e)}")
                
        except Exception as e:
            print(f"Error in cycle {cycles}: {str(e)}")

if __name__ == "__main__":    
    main()