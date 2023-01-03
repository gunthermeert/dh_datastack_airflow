import os
import json

# We're hardcoding these values here for the purpose of the demo, but in a production environment these
# would probably come from a config file and/or environment variables!
DBT_PROJECT_DIR = os.getenv('DBT_PROJECT_DIR') # DBT_PROJECT_DIR = /dh_datastack_dbt/dh_datastack
DBT_PROFILES_DIR = os.getenv('DBT_PROFILES_DIR') # DBT_PROFILES_DIR = /dh_datastack_dbt/.dbt
DBT_GLOBAL_CLI_FLAGS = "--no-write-json"
DBT_TARGET = os.getenv('DBT_TARGET')# DBT_TARGET = dev
model_run = "model.dh_datastack_marketing.stg_dh_datastack_mdm__customers"#"model.dh_datastack_marketing.stg_dh_datastack_mdm__customers


def load_manifest():
    local_filepath = "C:/Users/GuntherMeert/Downloads/manifest.json" #"/home/gunther/dh_datastack_dbt/dh_datastack/target/manifest.json"
    with open(local_filepath) as f:
        data = json.load(f)
    return data

def parent_mapping_data():
    data = load_manifest()
    return data["parent_map"]

#check if source tables have freshness on them
def source_freshness_nodes():
    source_freshness_nodes = []
    data = load_manifest()

    for node in data["sources"].keys():
        if node.split(".")[0] == "source":
            if data["sources"][node]["freshness"]['error_after']['count'] != None:
                source_freshness_nodes.append(node)

    return source_freshness_nodes

def generate_all_nodes():
    for node in data["nodes"].keys():
        if node.split(".")[0] == "model" or node.split(".")[0] == "snapshot" or node.split(".")[0] == "seed":
            dbt_nodes[node] = {}

            dbt_nodes[node]['node_name'] = node.split(".")[-1]
            dbt_nodes[node]['node_resource_type'] = data["nodes"][node]["resource_type"]

            # dependencies: the manifest file might generate duplicate dependencies within the same node
            # therefor we create a list with the distinct values
            node_dependencies = [x for x in data["nodes"][node]["depends_on"]["nodes"]]  # this removes dbt source dependencies of the original list data["nodes"][node]["depends_on"]["nodes"]
            node_dependencies_distinct = list(dict.fromkeys(node_dependencies))
            dbt_nodes[node]['node_depends_on'] = node_dependencies_distinct

            #check if we need to implement a source_freshness_check
            list_source_freshness_nodes = source_freshness_nodes()
            node_freshness_dependencies = [x for x in data["nodes"][node]["depends_on"]["nodes"] if "source." in x]

            dbt_nodes[node]['freshness_dependency'] = ""

            for node_freshness_dependency in node_freshness_dependencies:
                if node_freshness_dependency in list_source_freshness_nodes:
                    dbt_nodes[node]['freshness_dependency'] = node_freshness_dependency



def iterate_parent_nodes(node):
#iterate over every node it's dependencies to see if that node has more depencies, so we can build parents from parents
    if node.split(".")[0] == "model" or node.split(".")[0] == "snapshot" or node.split(".")[0] == "seed":
        dbt_nodes[node] = {}

        dbt_nodes[node]['node_name'] = node.split(".")[-1]
        dbt_nodes[node]['node_resource_type'] = node.split(".")[0]
        node_dependencies = [x for x in parent_map_data[node]]  # this removes dbt source dependencies of the original list data["nodes"][node]["depends_on"]["nodes"]
        node_dependencies_distinct = list(dict.fromkeys(node_dependencies))

        dbt_nodes[node]['node_depends_on'] = node_dependencies_distinct

        # check if we need to implement a source_freshness_check
        list_source_freshness_nodes = source_freshness_nodes()
        node_freshness_dependencies = [x for x in data["nodes"][node]["depends_on"]["nodes"] if "source." in x]

        dbt_nodes[node]['freshness_dependency'] = ""

        for node_freshness_dependency in node_freshness_dependencies:
            if node_freshness_dependency in list_source_freshness_nodes:
                dbt_nodes[node]['freshness_dependency'] = node_freshness_dependency

    for parent_node in parent_map_data[node]:
        iterate_parent_nodes(parent_node)


# add model/snapshot node info to dbt_nodes
dbt_nodes = {}

data = load_manifest()
#print(json.dumps(data, indent=1))
parent_map_data = parent_mapping_data()


#check if parameter model_run <> all
if model_run == 'all':
    generate_all_nodes()
else:
    if model_run in parent_map_data:
        iterate_parent_nodes(model_run)


print(dbt_nodes)

"""
# create a bashoperator per dbt node, this must be done before creating the airflow order dependency
airflow_operators = {}

for node in dbt_nodes:
    airflow_operators[node] = make_dbt_task(dbt_nodes[node]["node_name"], dbt_nodes[node]["node_resource_type"])
"""

