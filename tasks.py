from invoke import task

_TEST_FOLDER = "tests"
_SOURCE_FOLDERS = " ".join(["bq_schema", _TEST_FOLDER])


@task
def lint(context):
    context.run(f"pylint {_SOURCE_FOLDERS}")


@task
def type_check(context):
    context.run("mypy bq_schema")


@task
def check_code_format(context):
    context.run("black --check .")
    context.run("isort --profile black --check .")


@task
def test(context):
    context.run(
        f"pytest {_TEST_FOLDER} --doctest-modules --junitxml=junit/test-results.xml --cov=bq_schema --cov-report=xml --cov-report=html"
    )


@task
def format_code(context):
    context.run("black .")
    context.run("isort --profile black .")


@task(pre=[lint, type_check, check_code_format, test])
def check_all(_):
    pass
