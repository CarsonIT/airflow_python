import json
import pathlib
import airflow
import requests
import requests.exceptions as requests_exceptions
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

dag = DAG( 
 dag_id="test_rocket", 
 start_date=airflow.utils.dates.days_ago(14), 
 schedule_interval=None, 
 tags=["rocket_picture"]
)

# download_launches = BashOperator( 
#  task_id="download_launches", 
#  bash_command="""
#  curl -o /home/ubuntu/Downloads/launches_nofomat.json -L 'https://ll.thespacedevs.com/2.0.0/launch/upcoming';
#  python -m json.tool < /home/ubuntu/Downloads/launches_nofomat.json > /home/ubuntu/Downloads/launches.json;
#  """,
#  dag=dag,
# )


def _get_pictures():
    # Ensure target directory exists
    pathlib.Path("/home/ubuntu/Pictures/images").mkdir(parents=True, exist_ok=True)
    
    # Download all pictures in launches.json
    with open("/home/ubuntu/Downloads/launches.json") as f:
        launches = json.load(f)
        image_urls = [launch["image"] for launch in launches["results"] if launch.get("image")]
        for image_url in image_urls:
            try:
                image_filename = image_url.split("/")[-1]
                target_file = f"/home/ubuntu/Pictures/images/{image_filename}"

                response = requests.get(image_url)
                with open(target_file, "wb") as f:
                    f.write(response.content)

                print(f"Downloaded {image_url} to {target_file}")

            except requests_exceptions.MissingAASchema:
                print(f"{image_url} appears to be an invalid URL.")

            except requests_exceptions.ConnecAAtionError:
                print(f"Could not connect to {image_url}.")

        
 
get_pictures = PythonOperator( 
 task_id="get_pictures",
 python_callable=_get_pictures, 
 dag=dag,
)

notify = BashOperator(
 task_id="notify",
 bash_command='echo "There are now $(ls /home/ubuntu/Pictures/images/ | wc -l) images."',
 dag=dag,
)

get_pictures >> notify