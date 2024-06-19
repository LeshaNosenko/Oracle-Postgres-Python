import os
import tkinter as tk
from tkinter import filedialog
from lxml import etree


def add_files():
    files = filedialog.askopenfilenames()
    existing_files = list(file_listbox.get(0, tk.END))
    for file in files:
        if file not in existing_files:
            file_listbox.insert(tk.END, file)


def remove_drive(path):
    drive, path_without_drive = os.path.splitdrive(path)

    parts = path_without_drive.split('/')
    if parts[0] == '':
        parts = parts[1:]

    if 'db_scripts' in parts:
        parts.remove('db_scripts')

    return '/'.join(parts)


def make_xml_file():
    files = list(file_listbox.get(0, tk.END))

    databaseChangeLog = etree.Element(
        'databaseChangeLog',
        attrib={
            "xmlns": "http://www.liquibase.org/xml/ns/dbchangelog",
            "{http://www.w3.org/2001/XMLSchema-instance}schemaLocation":
                "http://www.liquibase.org/xml/ns/dbchangelog http://www.liquibase.org/xml/ns/dbchangelog/dbchangelog-2.0.xsd "
                "http://www.liquibase.org/xml/ns/dbchangelog-ext http://www.liquibase.org/xml/ns/dbchangelog/dbchangelog-ext.xsd"
        },
        nsmap={
            'xsi': 'http://www.w3.org/2001/XMLSchema-instance',
            'ext': 'http://www.liquibase.org/xml/ns/dbchangelog-ext'
        }
    )

    for file in files:
        has_create_or_replace = False
        with open(file, 'r', encoding='utf-8') as f:
            lines = f.readlines()

            for line in lines:
                if line.startswith("CREATE OR REPLACE") or line.startswith("create or replace") or line.startswith(
                        "DO") or line.startswith("do"):
                    has_create_or_replace = True
                    break

        file_path = remove_drive(file)
        if has_create_or_replace:
            changeSet = etree.SubElement(databaseChangeLog, 'changeSet',
                                         author=author.get(),
                                         id=os.path.splitext(os.path.basename(file))[0],
                                         context='all',
                                         runOnChange='true',
                                         runInTransaction='false')
            sqlFile = etree.SubElement(changeSet, 'sqlFile',
                                       path=file_path,
                                       splitStatements='false')
        else:
            changeSet = etree.SubElement(databaseChangeLog, 'changeSet',
                                         author=author.get(),
                                         id=os.path.splitext(os.path.basename(file))[0],
                                         context='all',
                                         runOnChange='true')
            sqlFile = etree.SubElement(changeSet, 'sqlFile',
                                       path=file_path,
                                       splitStatements='true',
                                       endDelimiter=';')

    pretty_databaseChangeLog = etree.tostring(databaseChangeLog, pretty_print=True, xml_declaration=True,
                                              encoding='UTF-8', standalone=False)

    filepath = filedialog.asksaveasfilename(filetypes=(("XML files", "*.xml"),))
    if filepath != "":
        with open(filepath, "w", encoding='utf-8') as output_file:
            output_file.write(pretty_databaseChangeLog.decode('utf-8'))


def make_release_xml_file():
    files = filedialog.askopenfilenames()

    if not files:
        return

    file = files[0]

    databaseChangeLog = etree.Element(
        'databaseChangeLog',
        attrib={
            "xmlns": "http://www.liquibase.org/xml/ns/dbchangelog",
            "{http://www.w3.org/2001/XMLSchema-instance}schemaLocation":
                "http://www.liquibase.org/xml/ns/dbchangelog http://www.liquibase.org/xml/ns/dbchangelog/dbchangelog-2.0.xsd "
                "http://www.liquibase.org/xml/ns/dbchangelog-ext http://www.liquibase.org/xml/ns/dbchangelog/dbchangelog-ext.xsd"
        },
        nsmap={
            'xsi': 'http://www.w3.org/2001/XMLSchema-instance',
            'ext': 'http://www.liquibase.org/xml/ns/dbchangelog-ext'
        }
    )

    for file in files:
        file_path = remove_drive(file)
        include = etree.SubElement(databaseChangeLog, 'include', file=file_path)

    pretty_databaseChangeLog = etree.tostring(databaseChangeLog, pretty_print=True, xml_declaration=True, encoding='UTF-8',
                                              standalone=False)

    filepath = filedialog.asksaveasfilename(filetypes=(("XML files", "*.xml"),))
    if filepath != "":
        with open(filepath, "w", encoding='utf-8') as output_file:
            output_file.write(pretty_databaseChangeLog.decode('utf-8'))


root = tk.Tk()

root.title("Формирование релизов Liquibase")

add_button = tk.Button(root, text="Выбрать файлы .sql для оформления в ликви xml", command=add_files)
add_button.pack(pady=10)

file_frame = tk.LabelFrame(root, text="Файлы")
file_frame.pack(pady=10)

file_listbox = tk.Listbox(file_frame, width=65)
file_listbox.pack()

autor_label = tk.Label(root, text='Укажите автора Миграции перед формированием xml')
autor_label.pack()

author = tk.Entry(root)
author.pack()

author.insert(0, "Nosenko")

make_files_button = tk.Button(root, text="Сформировать файл xml задачи на основе файлов .sql", command=make_xml_file)
make_files_button.pack(pady=10)

make_release_file_button = tk.Button(root, text="Сформировать файл релиза на основе файла задачи .xml",
                                     command=make_release_xml_file)
make_release_file_button.pack(pady=10)


root.mainloop()
