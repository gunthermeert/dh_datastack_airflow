import os
import json

# We're hardcoding these values here for the purpose of the demo, but in a production environment these
# would probably come from a config file and/or environment variables!
DBT_PROJECT_DIR = os.getenv('DBT_PROJECT_DIR') # DBT_PROJECT_DIR = /dh_datastack_dbt/dh_datastack
DBT_PROFILES_DIR = os.getenv('DBT_PROFILES_DIR') # DBT_PROFILES_DIR = /dh_datastack_dbt/.dbt
DBT_GLOBAL_CLI_FLAGS = "--no-write-json"
DBT_TARGET = os.getenv('DBT_TARGET')# DBT_TARGET = dev


model_run = "int_finance__product_sales"

def load_manifest():
    local_filepath = "C:/Users/GuntherMeert/Downloads/manifest.json" #"/home/gunther/dh_datastack_dbt/dh_datastack/target/manifest.json"
    with open(local_filepath) as f:
        data = json.load(f)
    return data


# load manifest file to compile airflow code
data = load_manifest()
# print(json.dumps(data, indent=1))

# add model/snapshot node info to dbt_nodes
dbt_nodes = {}

#check if parameter model_run <> all
if model_run == 'all':
    for node in data["nodes"].keys():
        if node.split(".")[0] == "model" or node.split(".")[0] == "snapshot" or node.split(".")[0] == "seed":
            dbt_nodes[node] = {}

            dbt_nodes[node]['node_name'] = node.split(".")[-1]
            # dbt_nodes[node]['node_compiled'] = data["nodes"][node]["compiled"]
            dbt_nodes[node]['node_resource_type'] = data["nodes"][node]["resource_type"]

            # dependencies: the manifest file might generate duplicate dependencies within the same node
            # therefor we create a list with the distinct values
            node_dependencies = [x for x in data["nodes"][node]["depends_on"]["nodes"] if
                                 "source." not in x]  # this removes dbt source dependencies of the original list data["nodes"][node]["depends_on"]["nodes"]
            node_dependencies_distinct = list(dict.fromkeys(node_dependencies))

            dbt_nodes[node]['node_depends_on'] = node_dependencies_distinct
else:
    for node in data["parent_map"].keys(): #get the parents from the specified model
        if node.split(".")[-1] == model_run:

            if node.split(".")[0] == "model" or node.split(".")[0] == "snapshot" or node.split(".")[0] == "seed":
                dbt_nodes[node] = {}

                dbt_nodes[node]['node_name'] = node.split(".")[-1]
                dbt_nodes[node]['node_resource_type'] = node.split(".")[0]

                node_dependencies = [x for x in data["parent_map"][node] if
                                     "source." not in x]  # this removes dbt source dependencies of the original list data["nodes"][node]["depends_on"]["nodes"]
                node_dependencies_distinct = list(dict.fromkeys(node_dependencies))

                dbt_nodes[node]['node_depends_on'] = node_dependencies_distinct

                #we need to add every parent to dbt_nodes, otherwise we aren't able to create a bashoperator for this, which is needed for creating the dependencies.
                #however for these nodes we don't want to check parents.

                for dbt_node in node_dependencies_distinct:
                    dbt_nodes[dbt_node] = {}

                    dbt_nodes[dbt_node]['node_name'] = dbt_node.split(".")[-1]
                    dbt_nodes[dbt_node]['node_resource_type'] = dbt_node.split(".")[0]
                    dbt_nodes[dbt_node]['node_depends_on'] = []

print(dbt_nodes)

"""
# create a bashoperator per dbt node, this must be done before creating the airflow order dependency
airflow_operators = {}

for node in dbt_nodes:
    airflow_operators[node] = make_dbt_task(dbt_nodes[node]["node_name"], dbt_nodes[node]["node_resource_type"])
"""

