# Placeholder

Transforms your **BigQuery Schema** to a **Python dataclass** and vice versa.

### Installation

1) Clone the project.
2) Navigate into the cloned project.
3) Make a virtualenvironment with **python version >=3.7**

    `pipenv --python PYTHON_VERSION`
    ```
    $ pipenv --python 3.7
    ```
    or

    `virtualenv -p /PATH_TO_PYTHON/ /DESIRED_PATH/VENV_NAME`
    ```
    $ virtualenv -p /usr/bin/python3.7 placeholder
    ```

4) Install flit via pip
    ```
    $ pip install flit
    ```
5) Install packages

    ```
    $ flit install --symlink
    ```