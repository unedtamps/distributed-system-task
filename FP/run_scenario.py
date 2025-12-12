import os
import subprocess
import sys
import time

def clear():
    os.system('clear')

def print_menu():
    clear()
    print("===== FP SISTEM TERDISTRIBUSI =====")
    print()
    print("PRIMARY-ASYNC Scenario:")
    print("  1. Scenario 1 - Performance Test")
    print("  2. Scenario 4 - Network Partition")
    print()
    print("GROUP-REP Scenario:")
    print("  3. Scenario 2")
    print("  4. Scenario 3")
    print()
    print("Run All Scenario:")
    print("  5. Run All Primary-Async Tests")
    print("  6. Run All Group-Rep Tests")
    print()
    print("Docker :")
    print("  7. Setup/Reset Primary-Async")
    print("  8. Setup/Reset Group-Rep")
    print("  9. Check Docker Status")
    print()
    print("  0. Exit")
    print()

def run_test(test_path, cwd=None):
    print(f"\nRunning {test_path}...\n")
    if cwd:
        result = subprocess.run([sys.executable, test_path], cwd=cwd)
    else:
        result = subprocess.run([sys.executable, test_path])
    print("-" * 50)
    if result.returncode == 0:
        print("\nTest selesai!")
    else:
        print("\nTest error!")

def check_containers():
    result = subprocess.run(
        ["docker", "ps", "--filter", "name=mysql"],
        capture_output=True,
        text=True
    )
    print(result.stdout)

def setup_primary_async():
    print("\nSetup Primary-Async...")

    print("Stopping containers...")
    subprocess.run(["docker", "compose", "down"], capture_output=True, cwd="primary-async")
    time.sleep(2)

    print("Starting containers...")
    subprocess.run(["docker", "compose", "up", "-d"], capture_output=True, cwd="primary-async")
    time.sleep(5)

    print("Setup replication...")
    subprocess.run([sys.executable, "primary-async/setup_primary-async.py"])

def setup_group_rep():
    print("\nSetup Group-Rep...")

    print("Stopping containers...")
    subprocess.run(["docker", "compose", "down"], capture_output=True, cwd="group-rep")
    time.sleep(2)

    print("Starting containers...")
    subprocess.run(["docker", "compose", "up", "-d"], capture_output=True, cwd="group-rep")
    time.sleep(5)

    print("Setup replication...")
    subprocess.run([sys.executable, "group-rep/setup_group.py"])

def run_all_primary_async():
    print("\nJalankan semua Primary-Async tests...")

    tests = [
        "scenario_1.py",
        "scenario_4.py"
    ]

    for i, test in enumerate(tests, 1):
        print(f"\nTest {i}/{len(tests)}: {test}")
        run_test(test, cwd="primary-async")

        if i < len(tests):
            print("\nWait ...")
            time.sleep(5)

def run_all_group_rep():
    print("\nJalankan semua Group-Rep tests...")

    tests = [
        "scenario_2.py",
        "scenario_3.py"
    ]

    for i, test in enumerate(tests, 1):
        print(f"\nTest {i}/{len(tests)}: {test}")
        run_test(test, cwd="group-rep")

        if i < len(tests):
            print("\nWait ...")
            time.sleep(5)

def main():
    tests = {
        '1': ("scenario_1.py", "primary-async"),
        '2': ("scenario_4.py", "primary-async"),
        '3': ("scenario_2.py", "group-rep"),
        '4': ("scenario_3.py", "group-rep")
    }

    while True:
        print_menu()
        choice = input("Pilih [0-9]: ").strip()

        if choice == '0':
            print("\nExit\n")
            break
        elif choice in ['1', '2', '3', '4']:
            test_path, cwd = tests[choice]
            run_test(test_path, cwd=cwd)
            input("\nPress Enter...")
        elif choice == '5':
            run_all_primary_async()
            input("\nPress Enter...")
        elif choice == '6':
            run_all_group_rep()
            input("\nPress Enter...")
        elif choice == '7':
            setup_primary_async()
            input("\nPress Enter...")
        elif choice == '8':
            setup_group_rep()
            input("\nPress Enter...")
        elif choice == '9':
            check_containers()
            input("\nPress Enter...")
        else:
            print("\nInvalid")
            time.sleep(1)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n\n")
    except Exception as e:
        print(f"\nError: {e}\n")
