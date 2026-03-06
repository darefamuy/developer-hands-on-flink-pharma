To run Flink SQL statements on Confluent Cloud you don’t create a “cluster” yourself; instead you:

1. **Create / use an environment & Kafka cluster**
2. **Create a Flink compute pool** (the Flink “cluster” abstraction)
3. **Connect via Console or CLI** and run statements against Kafka topics

Below is a concise, step‑by‑step guide.

---

## 1. Prerequisites

1. **Confluent Cloud account** and org access.  
2. **RBAC:** you need **FlinkAdmin**, **EnvironmentAdmin**, or **OrganizationAdmin** to create compute pools; **FlinkDeveloper** is enough to just use an existing pool.  
3. **At least one Kafka cluster** in the region where you want to run Flink.

---

## 2. Create a Flink compute pool (UI path)

A **compute pool** is the serverless Flink “cluster” that runs your SQL statements.

1. Log in to [Confluent Cloud](https://confluent.cloud).  
2. In the left nav, go to **Environments**, then click the target **environment tile**.  
3. Click **Flink** in the environment menu.  
4. On the Flink page, stay on **Compute pools** and click **Create compute pool**.  
5. **Region:**
   - Choose the **same cloud + region** as the Kafka cluster whose topics you want to query.  
6. **Pool name:** e.g. `my-flink-pool`.  
7. **Max CFUs:**
   - Set e.g. **10** CFUs (you can scale up later, not down).  
8. Click **Continue → Finish**.  
9. The pool appears in state **PROVISIONING**, then **RUNNING** after a few minutes.  

That’s all you need for the “cluster” side; the pool is serverless and auto‑scales to zero when idle.

---

## 3. (Alternative) Create a compute pool via CLI

If you prefer CLI/IaC, you can create the same pool from a terminal.

1. **Install / update CLI** and log in:

   ```bash
   confluent update --yes        # optional but recommended
   confluent login --prompt --save
   ```

2. **Set basic environment variables** (example):

   ```bash
   export COMPUTE_POOL_NAME="my-flink-pool"
   export CLOUD_PROVIDER="aws"
   export CLOUD_REGION="us-east-1"
   export ENV_ID="env-xxxxxx"
   export MAX_CFU="10"
   ```

3. **Create the pool**:

   ```bash
   confluent flink compute-pool create ${COMPUTE_POOL_NAME} \
     --cloud ${CLOUD_PROVIDER} \
     --region ${CLOUD_REGION} \
     --max-cfu ${MAX_CFU} \
     --environment ${ENV_ID}
   ```
   This returns an ID like `lfcp-xxd6og` and status `PROVISIONING` → `PROVISIONED`.  

4. Optionally set it as the **current pool** for subsequent commands:

   ```bash
   confluent environment use ${ENV_ID}
   confluent flink compute-pool use lfcp-xxd6og
   ```

---

## 4. Start the Flink SQL shell and run statements (CLI)

Once the compute pool is **RUNNING**, you can open the **Flink SQL shell** and execute SQL.

1. **Start shell** (either with explicit IDs or after `compute-pool use`):

   ```bash
   confluent flink shell \
     --environment ${ENV_ID} \
     --compute-pool lfcp-xxd6og
   ```  

   - For production, also specify `--service-account` to run statements under a service identity.  

2. In the shell, you can:

   - List catalogs / tables:
     ```sql
     SHOW CATALOGS;
     SHOW DATABASES;
     SHOW TABLES;
     ```
   - Create a test table and query it (simple example from docs):

     ```sql
     CREATE TABLE quickstart(message STRING);
     INSERT INTO quickstart VALUES ('hello world');
     SELECT * FROM quickstart;
     ```  

3. Exit the shell with `\q` or Ctrl‑D; you can reattach later with `confluent flink shell` using the same env/pool IDs.  

---

## 5. Create a Flink workspace in the Console (UI path)

You can also run Flink SQL from the **Stream processing → Workspaces** UI, which will auto‑create a compute pool if needed.

1. In Confluent Cloud, click **Stream processing** in the nav.  
2. Select the **environment** that hosts the Kafka topics you want to query.  
3. Click **Create new workspace**:
   - Pick **cloud provider** and **region** (match your Kafka cluster region).  
   - Click **Create workspace**.  

   Under the hood this provisions a compute pool and wires the workspace to it automatically.  

4. When the workspace’s compute pool shows **Running**, paste and run Flink SQL in the cell; results appear in-place.  

5. You can change the compute pool used by the workspace in **workspace settings → Compute pool selection** if you’ve created multiple pools.  
