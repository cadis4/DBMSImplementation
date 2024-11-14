# db_catalog.py
import xml.etree.ElementTree as ET

DATABASE_FILE = 'Database.xml'

def load_catalog():
    try:
        tree = ET.parse(DATABASE_FILE)
        return tree
    except ET.ParseError:
        print("Error: Database.xml is not well-formed.")
        return None

def save_catalog(tree):
    tree.write(DATABASE_FILE, encoding='utf-8', xml_declaration=True)
