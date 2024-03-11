import csv
import os
import re
import logging
from typedb.driver import TypeDB, TypeDBDriver, SessionType, TransactionType, Transitivity

# Connection parameters
SERVER_ADDR = 'localhost:1729'
DATABASE_NAME = 'sample_app'

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(funcName)s - %(message)s')
logger = logging.getLogger(__name__)


def connect_to_typedb_core(addr) -> TypeDBDriver:
    return TypeDB.core_driver(addr)


def generate_new_folder_name(path, folder) -> str:
    # Check if the folder with such name exists already
    full_path = os.path.join(path, folder)
    logger.debug(f"{ full_path=}")
    if not os.path.isdir(full_path):
        return full_path
    # Check if the folder name ends with a number preceded by an underscore
    match = re.match(r"(.*)_(\d+)$", folder)
    if match:
        # Folder name already ends with a number, so increment this number by one
        base_name, num = match.groups()
        new_folder_name = f"{base_name}_{int(num) + 1}"
        logger.debug(f"Incrementing: { new_folder_name=}")
    else:
        # Folder name does not end with a number, so we append _2 to its name
        new_folder_name = f"{folder}_2"
        logger.debug(f"Adding _2: { new_folder_name=}")
    # recursive call to check if the new folder name exists and repeat the process
    return generate_new_folder_name(path, new_folder_name)


def create_folder(db_name, schema) -> str:
    try:
        parent_dir = os.getcwd()
        logger.debug(f"{parent_dir=}")
        logger.debug(f"{db_name=}")
        new_folder_name = generate_new_folder_name(parent_dir, db_name)
        os.mkdir(new_folder_name)
        logger.debug(f"{   new_folder_name=}")
        os.mkdir(new_folder_name + "/entities")
        os.mkdir(new_folder_name + "/relations")
        with open(new_folder_name + "/schema.tql", 'w') as f:
            f.write(schema)
        return new_folder_name
    except OSError as error:
        logger.error(error)
        exit()


def build_row(tx, data_instance, fields):
    logger.debug("   Adding entity to a table")
    result = {"IID": data_instance.get_iid()}
    for column in fields:
        logger.debug(f"{column=}")
        if column != "IID":
            result[column] = ''
            owned_attr = tx.concepts.get_attribute_type(column).resolve()
            logger.debug(f"{owned_attr=}")
            attr_list = data_instance.get_has(tx, attribute_type=owned_attr)
            if tx.concepts.get_attribute_type(column).resolve().is_string():
                for attr in attr_list:
                    if result[column] == '':
                        result[column] = '"' + attr.as_attribute().get_value() + '"'
                    else:
                        result[column] += ';"' + attr.as_attribute().get_value() + '"'
            else:
                for attr in attr_list:
                    if result[column] == '':
                        result[column] = attr.as_attribute().get_value()
                    else:
                        result[column] += ';' + attr.as_attribute().get_value()
    return result


def export_entities(tx, folder_name, entity_type):
    fields = ["IID"]
    for attr in entity_type.get_owns(tx):
        attr_label = attr.get_label().name
        fields.append(attr_label)
        logger.debug(f"{ attr_label=}")
    path = folder_name + "/" + entity_type.get_label().name + '.csv'
    try:
        with open(path, 'w', newline='') as file:
            writer = csv.DictWriter(file, fieldnames=fields)
            writer.writeheader()
            for data_instance in entity_type.get_instances(tx, Transitivity.EXPLICIT):
                row = build_row(tx, data_instance, fields)
                logger.debug(f"{row=}")
                writer.writerow(row)
    except OSError as error:
        logger.error(error)
        exit()


def export_relations(tx, folder_name: str, relation_type):
    fields = ["IID"]
    for attr in relation_type.get_owns(tx):
        attr_label = attr.get_label().name
        fields.append(attr_label)
        logger.debug(f"{ attr_label=}")
    try:
        path = folder_name + "/" + relation_type.get_label().name + '.csv'
        with open(path, 'w', newline='') as file:
            writer = csv.DictWriter(file, fieldnames=fields)
            writer.writeheader()
            role_path = folder_name + "/" + relation_type.get_label().name + '__roles.csv'
            with open(role_path, 'w', newline='') as roles_file:
                role_fields = ["Relation", "Role", "Player"]
                role_writer = csv.DictWriter(roles_file, fieldnames=role_fields)
                role_writer.writeheader()
                for data_instance in relation_type.get_instances(tx, Transitivity.EXPLICIT):
                    row = build_row(tx, data_instance, fields)
                    logger.debug(f"{row=}")
                    writer.writerow(row)
                    # Collecting role players
                    if relation_type.is_relation_type():
                        for role in relation_type.as_relation_type().get_relates(tx):
                            for roleplayer in data_instance.as_relation().get_players_by_role_type(tx, role):
                                role_row = {"Relation": data_instance.get_iid(),
                                            "Role": role.get_label().name,
                                            "Player": roleplayer.get_iid()}
                                logger.debug(f"{role_row=}")
                                role_writer.writerow(role_row)
    except OSError as error:
        logger.error(error)
        exit()


def main():
    # Connect to TypeDB
    with connect_to_typedb_core(SERVER_ADDR) as connection:
        # Get the schema of the database and save it as a file
        schema = connection.databases.get(DATABASE_NAME).schema()
        new_folder: str = create_folder(DATABASE_NAME, schema)
        with connection.session(DATABASE_NAME, SessionType.SCHEMA) as session:
            with session.transaction(TransactionType.READ) as tx:
                entity_types = tx.concepts.get_root_entity_type()\
                    .get_subtypes(tx, transitivity=Transitivity.TRANSITIVE)
                for entity_type in entity_types:
                    type_name: str = entity_type.get_label().name
                    logger.debug(f"{type_name=}")
                    # Creating a single csv file for every entity type
                    export_entities(tx, new_folder + "/entities", entity_type)
                relation_types = tx.concepts.get_root_relation_type()\
                    .get_subtypes(tx, transitivity=Transitivity.TRANSITIVE)
                for relation_type in relation_types:
                    type_name: str = relation_type.get_label().name
                    logger.debug(f"{type_name=}")
                    # Creating two csv files for every relation type: one additional for its role players
                    export_relations(tx, new_folder + "/relations", relation_type)
    logger.info("Program complete.")


if __name__ == "__main__":
    main()
