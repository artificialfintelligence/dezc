from etl_web_to_gcs_parameterized import etl_web_to_gcs_super
from prefect.deployments.deployments import Deployment
from prefect.infrastructure.container import DockerContainer

docker_container_block = DockerContainer.load("dezc-docker-block")

docker_dep = Deployment.build_from_flow(
    flow=etl_web_to_gcs_super,
    name="docker-flow",
    infrastructure=docker_container_block,
)

if __name__ == "__main__":
    docker_dep.apply()
