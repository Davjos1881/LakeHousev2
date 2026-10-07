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

Apaga pipe si desea liberar memoria (no borra datos) y levanta sql:

    docker compose --profile pipe stop
    
    docker pull trinodb/trino:440
    
    docker pull dremio/dremio-oss:25.2.0
    
    docker compose --profile sql up -d

Trino. Cuando docker compose logs trino --tail 3 muestre SERVER STARTED:

    docker exec trino_fhbd_2026 trino --execute "SHOW SCHEMAS FROM iceberg"

Sin tablas cargadas, debe salir solo information_schema. Que responda sin error confirma que Trino llega a Nessie. No confirma la lectura de datos en MinIO, porque eso solo se puede probar cuando existan tablas reales.

Dremio (http://localhost:9047): la primera vez pide crear un usuario administrador. Después, Add Source → Nessie:

Endpoint URL: http://nessie:19120/api/v2, sin autenticación.

Storage: AWS Access Key admin / password, Root path warehouse, y desmarcar Encrypt connection.

Connection Properties: fs.s3a.path.style.access = true, fs.s3a.endpoint = minio:9000, dremio.s3.compat = true.

Si la fuente se guarda sin error y aparece nessie en el árbol de fuentes, la conexión funciona. De nuevo, la lectura de datos se comprobará con las tablas reales.

Trino usa la API v1 de Nessie y Dremio la v2. Es correcto, no las iguale.
