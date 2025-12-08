## Primary Async

1. Install deps
    ```bash
    pip install -r requirements.txt
    ```
2. Run docker
    ```bash
    docker compose down -v
    docker compose up -d
    ```
3. Run Setup
    ```bash
    python3 setup_primary-async.py
    ```
4. Run Scenario 1
    ```bash
    python3 scenario_1.py
    ```
5. Stop docker
    ```bash
    docker compose down -v
    ```

## Group Replications

1. Install deps
    ```bash
    pip install -r requirements.txt
    ```
2. Run docker
    ```bash
    docker compose down -v
    docker compose up -d
    ```
3. Run Setup
    ```bash
    python3 setup_group.py
    ```
4. Run Scenario 2
    ```bash
    python3 scenario_2.py
    ```
- Stop primary node (node1)
    ```bash
    python3 controller.py stop node1
- Start node1 again
    ```bash
    python3 controller.py start node1
    ```
- Join node1 to the group

    ```bash
    python3 controller.py join node1
    ```
5. Run Scenario 3

    ```bash
    python3 scenario_3.py
    ```
6. Stop docker
    ```bash
    docker compose down -v
    ```
