from prefect.infrastructure.container import DockerContainer, ImagePullPolicy

# alternative to creating DockerContainer block in the UI
docker_block = DockerContainer(
    image="faridt/prefect:dezc",  # insert your image name and tag here
    image_pull_policy=ImagePullPolicy.ALWAYS,
    auto_remove=True,
)

res = docker_block.save("dezc-docker-block", overwrite=True)

print(res)
