
class TableStatement:

    def __init__(self, query):
        self.table_name = query.get('table')
        self.type = query.get('type')
        self.action = query.get('action', {})
        self.fields = query.get('fields')
        self.options = query.get('options', [])
        self.field_types = {}

    def create_fields_strings(self, action_type=None):

        field_strings = []

        for field in self.fields:
            
            field_name = field.get('name')
            field_type = field.get('type')

            field_string = '{field_name} {field_type}'.format(
                field_name=field_name,
                field_type=field_type
            )

            self.field_types[field_name] = field_type

            if field.get('length'):
                field_string = '{field_string} ( {field_length} )'.format(
                    field_string=field_string,
                    field_length=field.get('length')
                )
            
            if field.get('options'):
                field_string = '{field_string} {options}'.format(
                    field_string=field_string,
                    options=field.get('options')
                )

            if action_type:

                if action_type == 'add_column':
                    field_string = 'ADD COLUMN {field_name}'.format(
                        field_name=field_name
                    )

                elif action_type == 'drop_column':
                    field_string = 'DROP COLUMN IF EXISTS {field_name}'.format(
                        field_name=field_name
                    )

                elif action_type == 'rename_column':
                    field_string = 'RENAME COLUMN {field_name} TO {alt_name}'.format(
                        field_name=field_name,
                        alt_name=field.get('alt_name')
                    )

            field_strings.append(field_string)

        return ', '.join(field_strings)

    def assemble_create_statement(self):

        create_statement = 'CREATE TABLE IF NOT EXISTS {table_name} ('.format(
            table_name=self.table_name
        )

        if self.fields:
            create_statement += self.create_fields_strings()

        if self.options:
            options_strings = []
            for option in self.options:

                if option.get('type') == 'primary_keys':
                    options_strings.append(
                        self.assemble_primary_keys(
                            option.get('values')
                        )
                    )

                if option.get('type') == 'foreign_keys':
                    options_strings.append(
                        self.assemble_foreign_keys(
                            option.get('values')
                        )
                    )

            create_statement = '{create_statement}, {options_string}'.format(
                create_statement=create_statement,
                options_string=', '.join(options_strings)
            )

        return '{create_statement});'.format(create_statement=create_statement)

    def assemble_alter_table_statement(self):

        alter_statement = "ALTER TABLE IF EXISTS {table_name}".format(
            table_name=self.table_name
        )

        field_strings = self.create_fields_strings(
            action_type=self.action.get('action_type')
        )

        alter_statement = "{alter_statement} {field_strings}".format(
            alter_statement=alter_statement,
            field_strings=field_strings
        )

        return alter_statement

    def assemble_primary_keys(self, option):
        return 'PRIMARY KEY ( {primary_keys} )'.format(
            primary_keys=', '.join(option)
        )

    def assemble_foreign_keys(self, option):
        foreign_key_strings = [
            'FOREIGN KEY ( {field} ) REFERENCES {table} ( {field} )'.format(
                field=foreign_key.get('field'),
                table=foreign_key.get('table')
            ) for foreign_key in option
        ]

        return ', '.join(foreign_key_strings)

    def assemble_delete_statement(self):
        return 'DROP TABLE IF EXISTS {table_name};'.format(
            table_name=self.table_name
        )
