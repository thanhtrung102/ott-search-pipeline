from aws_cdk import Stack, Tags


def tag_stack(stack: Stack, env_name: str) -> None:
    Tags.of(stack).add("Project", "ott-search-pipeline")
    Tags.of(stack).add("Environment", env_name)
