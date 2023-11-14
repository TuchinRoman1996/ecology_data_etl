import xml.etree.ElementTree as ET

tree = ET.parse(r'C:\Users\HW1\Desktop\employees_data_2023_10_12.xml')
root = tree.getroot()

for record in root.findall('record'):
    id_company_element = record.find('IDКомпании')

    if id_company_element is not None and id_company_element.text == '101':
        id_company_element.text = '8'

tree.write(r'C:\Users\HW1\Desktop\done\employees_data_2023_10_12.xml')
