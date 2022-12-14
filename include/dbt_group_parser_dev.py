import json
import logging
import os
import subprocess
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup

DBT_DIR = "/home/gunther/dh_datastack_dbt/dh_datastack"
GLOBAL_CLI_FLAGS = "--no-write-json"
DBT_COMMAND = None

class DbtDagParser:
    """
    A utility class that parses out a dbt project and creates the respective task groups

    :param dag: The Airflow DAG
    :param dbt_global_cli_flags: Any global flags for the dbt CLI
    :param dbt_project_dir: The directory containing the dbt_project.yml
    :param dbt_profiles_dir: The directory containing the profiles.yml
    :param dbt_target: The dbt target profile (e.g. dev, prod)
    :param dbt_tag: Limit dbt models to this tag if specified.
    :param dbt_run_group_name: Optional override for the task group name.
    :param dbt_test_group_name: Optional override for the task group name.
    """

    def __init__(
        self,
        dag=None,
        dbt_global_cli_flags=None,
        dbt_project_dir=None,
        dbt_profiles_dir=None,
        dbt_target=None,
        dbt_tag=None,
        dbt_run_group_name="dbt_run",
        dbt_model_run=None,
    ):

        self.dag = dag
        self.dbt_global_cli_flags = dbt_global_cli_flags
        self.dbt_project_dir = dbt_project_dir
        self.dbt_profiles_dir = dbt_profiles_dir
        self.dbt_target = dbt_target
        self.dbt_tag = dbt_tag

        self.dbt_run_group = TaskGroup(dbt_run_group_name)
        self.dbt_model_run = dbt_model_run

        # load manifest file to compile airflow code
        self.data = self.load_manifest()
        # print(json.dumps(data, indent=1))

        self.parent_map_data = self.parent_mapping_data()

        # add model/snapshot node info to dbt_nodes
        self.dbt_nodes = {}


        # Parse the manifest and populate the two task groups
        self.make_dbt_task_groups()

    def load_manifest(self):
        local_filepath = "/home/gunther/dh_datastack_dbt/dh_datastack/target/manifest.json" #"C:/Users/GuntherMeert/Downloads/manifest.json"
        with open(local_filepath) as f:
            data = json.load(f)

        return data

    def parent_mapping_data(self):
        data = self.load_manifest()
        return data["parent_map"]

    def generate_all_nodes(self):
        for node in self.data["nodes"].keys():
            if node.split(".")[0] == "model" or node.split(".")[0] == "snapshot" or node.split(".")[0] == "seed":
                self.dbt_nodes[node] = {}

                self.dbt_nodes[node]['node_name'] = node.split(".")[-1]
                self.dbt_nodes[node]['node_resource_type'] = self.data["nodes"][node]["resource_type"]

                # dependencies: the manifest file might generate duplicate dependencies within the same node
                # therefor we create a list with the distinct values
                node_dependencies = [x for x in self.data["nodes"][node]["depends_on"]["nodes"] if
                                     "source." not in x]  # this removes dbt source dependencies of the original list data["nodes"][node]["depends_on"]["nodes"]
                node_dependencies_distinct = list(dict.fromkeys(node_dependencies))

                self.dbt_nodes[node]['node_depends_on'] = node_dependencies_distinct

    def iterate_parent_nodes(self, node):
        # iterate over every node it's dependencies to see if that node has more depencies, so we can build parents from parents
        if node.split(".")[0] == "model" or node.split(".")[0] == "snapshot" or node.split(".")[0] == "seed":
            self.dbt_nodes[node] = {}

            self.dbt_nodes[node]['node_name'] = node.split(".")[-1]
            self.dbt_nodes[node]['node_resource_type'] = node.split(".")[0]
            node_dependencies = [x for x in self.parent_map_data[node] if
                                 "source." not in x]  # this removes dbt source dependencies of the original list data["nodes"][node]["depends_on"]["nodes"]
            node_dependencies_distinct = list(dict.fromkeys(node_dependencies))

            self.dbt_nodes[node]['node_depends_on'] = node_dependencies_distinct

        for parent_node in self.parent_map_data[node]:
            self.iterate_parent_nodes(parent_node)

    def make_dbt_task(self, node_name, node_resource_type):
        """Returns an Airflow operator"""

        if node_resource_type == "model":
            DBT_COMMAND = "run"
        if node_resource_type == "snapshot":
            DBT_COMMAND = "snapshot"
        if node_resource_type == "seed":
            DBT_COMMAND = "seed"


        dbt_task = BashOperator(
            task_id=node_name,
            task_group=self.dbt_run_group,
            bash_command=f"""
            cd {DBT_DIR} &&
            dbt {GLOBAL_CLI_FLAGS} {DBT_COMMAND} --target dev --select {node_name} &&
            dbt {GLOBAL_CLI_FLAGS} test --target dev --select {node_name}
            """,
            dag=self.dag,
        )

        return dbt_task


    def make_dbt_task_groups(self):

        # check if parameter model_run <> all
        if self.dbt_model_run == 'all':
            self.generate_all_nodes()
        else:
            if self.dbt_model_run in self.parent_map_data:
                self.iterate_parent_nodes(self.dbt_model_run)

        #create a bashoperator per dbt node, this must be done before creating the airflow order dependency
        airflow_operators = {}

        for node in self.dbt_nodes:
            airflow_operators[node] = self.make_dbt_task(self.dbt_nodes[node]["node_name"], self.dbt_nodes[node]["node_resource_type"])

        #after creating the bash operators we must determine the scheduling order of the operators
        for node in self.dbt_nodes:

                node_dependencies = self.dbt_nodes[node]["node_depends_on"]

                # check if there are node dependancies, because nodes without dependencies could exist
                if(len(node_dependencies) == 0):
                    airflow_operators[node]
                else:
                    for dependency in node_dependencies:
                        airflow_operators[dependency] >> airflow_operators[node]

    def get_dbt_run_group(self):
        """
        Getter method to retrieve the previously constructed dbt tasks.

        Returns: An Airflow task group with dbt run nodes.

        """
        return self.dbt_run_group

