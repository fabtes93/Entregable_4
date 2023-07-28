# Este readme es una descripción de como realice el entregable:
* Dockerfile: cree un archivo dockerfile con información de librerías a instalar
* Se crearon las carpetas dentro del entregable: Airflow_docker -> mkdir -p dags,logs,plugins
* Se creo el archvio de docker-compose.yml 
* A la altura del archivo docker-compose.yml se crea la imagen de docker y se hace correr el container: docker run 
* Luego se levanta Airflow: docker-compose up airflow-init -> docker-compose up --build
* Una vez que los servicios estaban levantados, ingrese a Airflow en `http://localhost:8080/`.
* En la pestaña `Admin -> Connections` cree la conexión a Redshift dentro de Airflow con los siguientes datos:
    host
    port
    database
    user
    password
    table_name
* En la pestaña `Admin -> Variables` cree una nueva variable con los siguientes datos:
    SMTP_EMAIL_TO
    SMTP_EMAIL_FROM
    SMTP_PASSWORD
* Ejecuto el DAG


