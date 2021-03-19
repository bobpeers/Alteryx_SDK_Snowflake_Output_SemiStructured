"""
AyxPlugin (required) has-a IncomingInterface (optional).
Although defining IncomingInterface is optional, the interface methods are needed if an upstream tool exists.
"""

import AlteryxPythonSDK as Sdk
import xml.etree.ElementTree as Et
import cleaner
import time
import os
import glob
import snowflake.connector
import logging

VERSION = '1.0'


class AyxPlugin:
    """
    Implements the plugin interface methods, to be utilized by the Alteryx engine to communicate with a plugin.
    Prefixed with "pi", the Alteryx engine will expect the below five interface methods to be defined.
    """

    def __init__(self, n_tool_id: int, alteryx_engine: object, output_anchor_mgr: object):
        """
        Constructor is called whenever the Alteryx engine wants to instantiate an instance of this plugin.
        :param n_tool_id: The assigned unique identification for a tool instance.
        :param alteryx_engine: Provides an interface into the Alteryx engine.
        :param output_anchor_mgr: A helper that wraps the outgoing connections for a plugin.
        """

        # Default properties
        self.n_tool_id = n_tool_id
        self.alteryx_engine = alteryx_engine

        # Basic lists of text inputs
        self.input_list: list = ['account', 'user', 'password', 'warehouse', 'database', 'schema', 'table']

        # Create text box variables
        for item in self.input_list:
            setattr(AyxPlugin, item, None)

        self.auth_type: str = None
        self.okta_url: str = None
        self.sql_type: str = None
        self.temp_dir: str = None
        self.ss_data_field: str = None

        self.case_sensitive: bool = False
        self.suspend_wh: bool = False
        self.delete_tempfiles: bool = False

        self.is_initialized: bool = True
        self.single_input = None

    def pi_init(self, str_xml: str):
        """
        Handles input data verification and extracting the user settings for later use.
        Called when the Alteryx engine is ready to provide the tool configuration from the GUI.
        :param str_xml: The raw XML from the GUI.
        """
        # stop workflow is output tools are disbaled in runtime settings
        if self.alteryx_engine.get_init_var(self.n_tool_id, 'DisableAllOutput') == 'True':
            self.is_initialized = False
            return False

        # Getting the user-entered file path string from the GUI, to use as output path.
        root = Et.fromstring(str_xml)

        # Basic text inpiut list
        for item in self.input_list:
            setattr(AyxPlugin, item, root.find(item).text if item in str_xml else None)

        self.auth_type = root.find('auth_type').text  if 'auth_type' in str_xml else None
        self.okta_url = root.find('okta_url').text if 'okta_url' in str_xml else None
        self.temp_dir = root.find('temp_dir').text  if 'temp_dir' in str_xml else None
        self.sql_type = root.find('sql_type').text  if 'sql_type' in str_xml else None
        self.ss_data_field = root.find('ss_data_field').text  if 'ss_data_field' in str_xml else None

        self.case_sensitive = root.find('case_sensitive').text == 'True' if 'case_sensitive' in str_xml else False
        self.suspend_wh = root.find('supend_wh').text == 'True' if 'supend_wh' in str_xml else False

        # fix for listrunner sending line feeds and spaces
        self.okta_url = cleaner.sanitise_inputs(self.okta_url)
        self.temp_dir = cleaner.sanitise_inputs(self.temp_dir)
            
        # check for okta url is using okta
        if self.auth_type == 'okta':
            if not self.okta_url:
                self.display_error_msg(f"Enter a valid Okta URL when authenticating using Okta")
                return False 
            elif 'http' not in self.okta_url:
                self.display_error_msg(f"Supplied Okta URL is not valid")
                return False        

        # data checks
        for item in self.input_list:
            attr = getattr(AyxPlugin, item, None)
            attr = cleaner.sanitise_inputs(attr)
            if attr is None:
                self.display_error_msg(f"Enter a valid {item}")
                return False

        if not self.ss_data_field:
            self.display_error_msg(f'Map a valid filepath to the data files')

        # remove protocol if added
        if '//' in self.account:
            self.account = self.account[self.account.find('//') + 2:]

        self.password = self.password[::-1]

        # Check temp_dir and use Alteryx default if None
        if not self.temp_dir:
            self.temp_dir = self.alteryx_engine.get_init_var(self.n_tool_id, "TempPath")
            self.display_file(f'{self.temp_dir}| Using system temp dir {self.temp_dir}')
        else:
            error_msg = self.msg_str(self.temp_dir)
            if error_msg != '':
                self.display_error_msg(error_msg)
                return False

    def pi_add_incoming_connection(self, str_type: str, str_name: str) -> object:
        """
        The IncomingInterface objects are instantiated here, one object per incoming connection.
        Called when the Alteryx engine is attempting to add an incoming data connection.
        :param str_type: The name of the input connection anchor, defined in the Config.xml file.
        :param str_name: The name of the wire, defined by the workflow author.
        :return: The IncomingInterface object.
        """

        self.single_input = IncomingInterface(self)
        return self.single_input

    def pi_add_outgoing_connection(self, str_name: str) -> bool:
        """
        Called when the Alteryx engine is attempting to add an outgoing data connection.
        :param str_name: The name of the output connection anchor, defined in the Config.xml file.
        :return: True signifies that the connection is accepted.
        """

        return True

    def pi_push_all_records(self, n_record_limit: int) -> bool:
        """
        Called when a tool has no incoming data connection.
        :param n_record_limit: Set it to <0 for no limit, 0 for no records, and >0 to specify the number of records.
        :return: True for success, False for failure.
        """

        self.display_error_msg('Missing Incoming Connection')
        return False

    def pi_close(self, b_has_errors: bool):
        """
        Called after all records have been processed.
        :param b_has_errors: Set to true to not do the final processing.
        """

        pass

    def display_error_msg(self, msg_string: str):
        self.alteryx_engine.output_message(self.n_tool_id, Sdk.EngineMessageType.error, msg_string)
        self.is_initialized = False

    def display_info(self, msg_string: str):
        self.alteryx_engine.output_message(self.n_tool_id, Sdk.EngineMessageType.info, msg_string)

    def display_file(self, msg_string: str):
        self.alteryx_engine.output_message(self.n_tool_id, Sdk.Status.file_output, msg_string)

    @staticmethod
    def msg_str(file_path: str) -> str:
        """
        A non-interface, helper function that handles validating the file path input.
        :param file_path: The file path and file name input by user.
        :return: The chosen message string.
        """

        msg_str = ''
        if len(file_path) > 259:
            msg_str = 'Maximum path length is 259'
        elif any((char in set('/;?*"<>|')) for char in file_path):
            msg_str = 'These characters are not allowed in the file path: /;?*"<>|'
        elif not os.access(file_path, os.W_OK):
            msg_str = 'Unable to write to supplied temp path'
        return msg_str  

class IncomingInterface:
    """
    This optional class is returned by pi_add_incoming_connection, and it implements the incoming interface methods, to
    be utilized by the Alteryx engine to communicate with a plugin when processing an incoming connection.
    Prefixed with "ii", the Alteryx engine will expect the below four interface methods to be defined.
    """

    def __init__(self, parent: object):
        """
        Constructor for IncomingInterface.
        :param parent: AyxPlugin
        """
        # Default properties
        self.parent = parent

        # Custom membersn
        self.record_info_in = None
        self.field_list: list = []
        self.counter: int = 0
        self.timestamp: int = 0
        self.fld_index: int = None

    def ii_init(self, record_info_in: object) -> bool:
        """
        Handles the storage of the incoming metadata for later use.
        Called to report changes of the incoming connection's record metadata to the Alteryx engine.
        :param record_info_in: A RecordInfo object for the incoming connection's fields.
        :return: True for success, otherwise False.
        """
        field_name: str = ''

        if self.parent.alteryx_engine.get_init_var(self.parent.n_tool_id, 'UpdateOnly') == 'True' or not self.parent.is_initialized:
            return False

        self.parent.display_info(f'Running Snowflake JSON + XML Output version {VERSION}')
        self.record_info_in = record_info_in  # For later reference.

        # Storing the field index of the mapped path
        self.fld_index = record_info_in.get_field_num(self.parent.ss_data_field)

        self.timestamp = str(int(time.time()))
        self.parent.temp_dir = os.path.join(self.parent.temp_dir, self.timestamp)

        if not os.path.exists(self.parent.temp_dir):
            os.makedirs(self.parent.temp_dir)
        
        # Logging setup
        logging.basicConfig(filename=os.path.join(self.parent.temp_dir, 'snowflake_connector.log'), format='%(asctime)s - %(message)s', level=logging.INFO)

        return True

    def ii_push_record(self, in_record: object) -> bool:
        """
        Responsible for writing the data to csv in chunks.
        Called when an input record is being sent to the plugin.
        :param in_record: The data for the incoming record.
        :return: False if file path string is invalid, otherwise True.
        """

        if not self.parent.is_initialized:
            return False

        # store all paths in list
        in_value = self.record_info_in[self.fld_index].get_as_string(in_record)
        if in_value:
            self.counter += 1 
            self.field_list.append(in_value)

        return True
      
    def ii_update_progress(self, d_percent: float):
        """
         Called by the upstream tool to report what percentage of records have been pushed.
         :param d_percent: Value between 0.0 and 1.0.
        """
        self.parent.alteryx_engine.output_tool_progress(self.parent.n_tool_id, d_percent)  # Inform the Alteryx engine of the tool's progress

    def ii_close(self):
        """
        Handles writing out any residual data out.
        Called when the incoming connection has finished passing all of its records.
        """

        # for filepaths
        filename: str = ''
        ext: str = ''
        unique_exts: set = set()

        if self.parent.alteryx_engine.get_init_var(self.parent.n_tool_id, 'UpdateOnly') == 'True' or not self.parent.is_initialized:
            return False
        elif self.counter == 0:
            self.parent.display_info('No records to process')
            return False

        # check we only have one file type
        for f in self.field_list:
            filename, ext = os.path.splitext(f)
            unique_exts.add(ext[1:])
        
        if len(unique_exts) != 1:
            self.parent.display_error_msg('You may only upload one file type into a table')
            return False

        # get file extension
        ext = unique_exts.pop()

        # check for valid extentions
        if ext.lower() not in ['json', 'xml', 'parquet', 'avro', 'orc']:
            self.parent.display_error_msg(f'{ext} is not a supported file type')
            return False

        con: snowflake.connector.connection = None

        # Create Snowflake connection

        try:
            if self.parent.auth_type == 'snowflake':
                con = snowflake.connector.connect(
                                                user=self.parent.user,
                                                password=self.parent.password,
                                                account=self.parent.account,
                                                warehouse=self.parent.warehouse,
                                                database=self.parent.database,
                                                schema=self.parent.schema,
                                                ocsp_fail_open=True
                                                )
                self.parent.display_info('Authenticated via Snowflake')
            else:
                con = snowflake.connector.connect(
                                                user=self.parent.user,
                                                password=self.parent.password,
                                                authenticator=self.parent.okta_url,
                                                account=self.parent.account,
                                                warehouse=self.parent.warehouse,
                                                database=self.parent.database,
                                                schema=self.parent.schema,
                                                ocsp_fail_open=True
                                                )                
                self.parent.display_info('Authenticated via Okta')

            # Set warehouse and schema
            con.cursor().execute(f"USE WAREHOUSE {self.parent.warehouse}")
            con.cursor().execute(f"USE SCHEMA {self.parent.database}.{self.parent.schema}")

            # fix table name and field name if case sensitive used or keyswords
            self.parent.table = cleaner.reserved_words(self.parent.table, self.parent.case_sensitive)
            self.parent.ss_data_field = cleaner.reserved_words(self.parent.ss_data_field, self.parent.case_sensitive)
        
            # Execute Table Creation #
            if self.parent.sql_type == 'create':
                table_sql: str = f'Create or Replace table {self.parent.table}  ({self.parent.ss_data_field} VARIANT)'
                con.cursor().execute(table_sql)

            elif self.parent.sql_type == 'truncate':
                con.cursor().execute(f'truncate table {self.parent.table}')

            for f in self.field_list:
                f = f.replace('\\', '/')
                con.cursor().execute(f"PUT 'file://{f}' @%{self.parent.table} PARALLEL=64 OVERWRITE=TRUE")
            
            con.cursor().execute(f"COPY INTO {self.parent.table} FILE_FORMAT = (TYPE={ext} COMPRESSION=GZIP) PURGE = TRUE")

            self.parent.display_info(f'Processed {self.counter:,} records')
            
            if self.parent.suspend_wh:
                con.cursor().execute(f'alter warehouse {self.parent.warehouse} suspend')
                self.parent.display_info('Suspended the warehouse')

        except Exception as e:
            logging.error(str(e))
            self.parent.display_error_msg(f'Error {e.errno} ({e.sqlstate}): {e.msg} ({e.sfqid})')
        finally:

            if con:
                con.close()

        self.parent.display_info('Snowflake transaction complete')