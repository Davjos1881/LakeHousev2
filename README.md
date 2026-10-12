El contenedor Está hecho con estos recursos:

- Docker Desktop con Compose v2.17 o superior (docker compose version).
- 10 GB de memoria para Docker. En Windows, crea C:\Users\<usuario>\.wslconfig con [wsl2] y memory=10GB, ejecuta wsl --shutdown y abre Docker Desktop.
- 20 GB libres en disco y conexión a internet (solo para el primer build).

Instrucciones:

Desde la carpeta del proyecto y una a la vez:

    docker build -f docker/jars/Dockerfile -t lakehouse-jars:1.0 .
    
    docker build -f docker/lakehouse/Dockerfile -t lakehouse:1.0 .
    
    docker images | findstr lakehouse

  Deben aparecer **lakehouse-jars:1.0** y **lakehouse:1.0.** La segunda tarda bastante.

  2. Bajar las imágenes oficiales (esto es medio opcional, solo en caso de que alguna falle, saber cual es)

    docker pull postgres:15
    
    docker pull ghcr.io/projectnessie/nessie:0.77.1
    
    docker pull pgsty/minio:RELEASE.2026-06-18T00-00-00Z
    
    docker pull amazon/aws-cli:2.17.0
    
    docker pull apache/spark:3.5.5-scala2.12-java17-python3-ubuntu

  3. Levantar por etapas

    docker compose up -d
    
    docker compose ps -a

  **spark_jars** y **airflow_init** terminan en Exited (0), que es lo normal. Airflow tarda varios minutos en estar listo.

  4. Comprobar los servicios

**Servicio**	   **URL**	              **Acceso**	             **Qué debe verse**

Spark	       http://localhost:9090                               Master y 1 worker ALIVE

MinIO	       http://localhost:9001	     admin / password	       Buckets bronze y warehouse

Airflow	     http://localhost:8080	     admin / admin	         La interfaz abre

Jupyter	     http://localhost:8888	     token lakehouse	       JupyterLab abre

Nessie	     http://localhost:19120                              La interfaz carga

Y que los JARs llegaron al volumen:

    docker exec spark_worker_fhbd_2026 ls /opt/extra-jars

Deben aparecer 5 archivos .jar.

5. Trino y Dremio

Apaga pipe si deseas liberar memoria (no borra datos) y levanta sql:

    docker compose --profile pipe stop
    
    docker pull trinodb/trino:440
    
    docker pull dremio/dremio-oss:25.2.0
    
    docker compose --profile sql up -d --no-build

Trino. Cuando docker compose logs trino --tail 3 muestre SERVER STARTED:

    docker exec trino_fhbd_2026 trino --execute "SHOW SCHEMAS FROM iceberg"

Sin tablas cargadas, debe salir solo information_schema. Que responda sin error confirma que Trino llega a Nessie. No confirma la lectura de datos en MinIO, porque eso solo se puede probar cuando existan tablas reales.

Luego, entra a http://localhost:8081, deberias ver la interfaz de Trino

<img width="1917" height="937" alt="image" src="https://github.com/user-attachments/assets/fc0683e1-44a7-4b6a-b8ea-229626495dd2" />

Una vez insertes un usuario (el que sea), veras el Ui:

<img width="1917" height="937" alt="image" src="https://github.com/user-attachments/assets/fda16478-cb80-4678-b03b-94dc732d8e25" />



Para Dremio (http://localhost:9047): la primera vez pide crear un usuario administrador. 

Después, Add Source --> Nessie:

Endpoint URL: http://nessie:19120/api/v2, sin autenticación.

Storage: AWS Access Key admin / password, Root path warehouse, y desmarcar Encrypt connection.

Connection Properties: 

fs.s3a.path.style.access = true

fs.s3a.endpoint = minio:9000 

dremio.s3.compat = true.

Si la fuente se guarda sin error y aparece nessie en el árbol de fuentes, la conexión funciona. De nuevo, la lectura de datos se comprobará con las tablas reales.

Trino usa la API v1 de Nessie y Dremio la v2. Es correcto, no las iguales.

**Instruciones Para la ingesta manual de  post_2020, post_2021 y users_2021**

Con el perfil core y pipe activo:

    docker compose up -d

    docker compose --profile pipe up -d --no-build

Ejecutar en la consola y en el mismo orden:

**IMPORTANTE: cada proceso de esta ingesta manual puede durar varios minutos, en mi caso fueron casi 8 o 10 minutos**

    docker exec jupyter_notebook_fhbd_2026 /home/airflow/dlt-venv/bin/python /workspace/jobs/bronze_seed.py post 2020_H1
    
    docker exec jupyter_notebook_fhbd_2026 /home/airflow/dlt-venv/bin/python /workspace/jobs/bronze_seed.py posts 2021_H1

    docker exec jupyter_notebook_fhbd_2026 /home/airflow/dlt-venv/bin/python /workspace/jobs/bronze_seed.py users 2020_H1

En caso de la carga de Users del 2021, puedes hacerlo directamente desde Jupyter en http://localhost:8888  corriendo el notebook **bronze_ingest.ipynb** o mediante el Dag en Airflow

verificar que en minio el bucket tenga los archivos y carpetas correctas, o tambien puedes ver lo subido con el comando:

    docker exec jupyter_notebook_fhbd_2026 /home/airflow/dlt-venv/bin/python /workspace/jobs/bronze_ls.py

**Ingesta con Script de Posts a Silver**

    docker exec jupyter_notebook_fhbd_2026 python /workspace/jobs/silver_posts.py

Puedes verificarlo directamente en el Minio o poniendo esto en la consola

    docker exec jupyter_notebook_fhbd_2026 python -c "from pyspark.sql import SparkSession; s=SparkSession.builder.getOrCreate(); s.sql('SELECT check_name, failed_rows, total_rows, action FROM nessie.silver.dq_log').show(truncate=False); s.sql('SELECT count(*) n, count(distinct post_id) ids FROM nessie.silver.posts_hist').show(); s.stop()"

Ejecutalo dos o más veces, las filas de posts_hist deben ser las mismas, y esa es la prueba de que el MERGE es idempotente

En caso de la carga de Users a Silver, puedes hacerlo directamente desde Jupyter en http://localhost:8888  corriendo el notebook **silver_transform.ipynb** o mediante el Dag en Airflow

Para la capa Gold es mas de lo mismo, directamente desde Jupyter en corriendo el notebook **gold_agg.ipynb** o mediante el Dag en Airflow

Una vez el Dag finalice (en mi caso han sido alrededor de 5 minutos por run) para ver la salida de Gold, selecciona la tarea **gold_posts_counts_by_user** y en la seccion de logs deberias ver:

<img width="800" height="257" alt="image" src="https://github.com/user-attachments/assets/575992d0-fa85-4fad-8cbb-9af888d0e354" />

<img width="812" height="512" alt="image" src="https://github.com/user-attachments/assets/98c8b47e-0918-4ebb-bb47-69c8584de402" />

<img width="952" height="428" alt="image" src="https://github.com/user-attachments/assets/76a17379-50d1-4b24-9ab4-8f0c882454c4" />

En este punto, podemos apagar el perfil pipe del contenedor si lo deseamos. Aunque no es estrictamente necesario

    docker compose --profile pipe down

Ahora, a través de consultas a Silver en Dremio y Trino debemos replicar la misma informacion de Gold

Para ello, encendemos el perfil sql del contenedor

    docker compose --profile sql up -d --no-build

En Dremio (http://localhost:9047/), simplemente abre el SQL Editor y pega la consulta del archivo **dremio_gold.sql** ubicado en la carpeta sql del proyecto

En trino es algo diferente, ya que aqui usaremos **Dbeaver** y luego de eso crearemos una conexion a Trino para hacer la consulta

Una vez insertemos un usuario en Trino y estemos observando la Ui, nos dirigimos a Dbeaver y creamos una nueva conexion

<img width="1917" height="1078" alt="image" src="https://github.com/user-attachments/assets/2c38b82f-a913-4b3c-af26-395215547ff6" />

El usuario puede ser cualquiera, lo mismo con la contraseña. Lo importante es que el puerto sea el 8081

Dale a Probar conexion y si no hay errores, dale a finalizar

Con la conexion creada y activa (dale doble clic sobre ella, debe aparecer el chulo)

<img width="482" height="637" alt="image" src="https://github.com/user-attachments/assets/362e45d1-cda9-402c-bf31-c1c4ccb0bba3" />

Crea un nuevo script sql y pega el script 2 del archivo **trino_silver_y_replica_gold.sql** de la carpeta sql del proyecto

Una vez ejecutes el comando y mientras se completa el proceso, puede ver en el Ui de Trino y en Dbeaver el proceso

Ya terminada la consulta, puedes ver la salida en la consola de Dbeaver. La consulta tira varias columnas, pero las importantes son:

total_posts (por usuario)

<img width="165" height="333" alt="image" src="https://github.com/user-attachments/assets/f859b3a4-8f87-4342-a453-234f89a865eb" />

activity_tier y activity_trend (por usuario)

<img width="362" height="340" alt="image" src="https://github.com/user-attachments/assets/d1d3fa28-9c3e-4da2-816b-65d2c7cc06eb" />


Para Apagar el contenedor:

    docker compose --profile pipe --profile sql down
    
    docker compose down

Con el contenedor apagado, podemos verificar que los volúmenes siguen ahí:

    docker volume ls --filter name=lakehousev2

Deberia aparecer:

DRIVER    VOLUME NAME

local     lakehousev2_dremio-data

local     lakehousev2_minio-data

local     lakehousev2_postgres-data

local     lakehousev2_spark-jars
