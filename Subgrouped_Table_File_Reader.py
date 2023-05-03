"""
SUBGROUPED TABLE FILE READER
(version 1.0)
by Angelo Chan

This module contains a Class capable of reading and a data table file whose
contents are sorted by subgroup, and returns the subgroups as a list of list of
values, rather than returning individual lines.

Example file contents:

    ID  GROUP_ID    SUBGROUP_ID    VALUE
    1   Tree        Oak
    2   Tree        Willow
    3   Tree        Maple
    4   Ore         Copper
    5   Ore         Tin
    6   Ore         Iron

Example yields:

    What the reader returns after the first read:
        [["1", "Tree", "Oak"], ["2", "Tree", "Willow"], ["3", "Tree", "Maple"]]

    What the reader returns after the second read:
        [["4", "Ore", "Copper"], ["5", "Ore", "Tin"], ["6", "Ore", "Iron"]] 
"""

# Imported Modules #############################################################

from Table_File_Reader import *



# Lists ########################################################################



# Dictionaries #################################################################



# Classes ######################################################################

class Subgrouped_Table_Reader(Table_Reader):
    """
    The Subgrouped Table Reader is a file reader designed specifically to work
    with CSV, TSV, or SSV files whose contents are sorted into groups based on
    a "group ID".

    The Table Reader acts as a two-phase buffer to store both the "current"
    group of values and the "next" row, not every row in the file.

    The Subgrouped Table Reader defaults to Tab Separated Format behaviour.

    Designed for the following use:
    
    f = Subgrouped_Table_Reader()
    f.Set_New_Path("F:/Filepath.csv")
    f.Autodetect_Delimiter() # OR f.Set_Delimiter(",")
    f.Set_Group_ID_Column_No(0)
    f.Set_Enclosers(["\"", "'"])  # Optional
    f.Set_Keep_Enclosers(True)    # Optional
    f.Set_Header_Params(["#", 1]) # Optional
    #                               Skip:
    #                                   all rows starting with "#", THEN
    #                                   1 row
    f.Open()
    
    # f.Read() # Optional for headers
    
    while not f.EOF:
        f.Read()
        # Your code - You may access buffered elements in f
    f.Close()
    """
    
    # Minor Configurations #####################################################
    
    empty_element = [[""]]
    
    # Minor Configurations #####################################################
    
    _CONFIG__print_errors = True
    _CONFIG__print_progress = False
    _CONFIG__print_metrics = True
    
    
    
    # Strings ##################################################################
    
    _MSG__object_type = "Subgrouped Table File Reader"
    
    _MSG__invalid_col_group_ID = "Invalid column number specified for the "\
            "group ID.\nPlease specify a non-negative integer."
    
    _MSG__no_col_group_ID = "No column number has been specified as containing"\
            " the group IDs."
    
    
    
    # Constructor & Destructor #################################################
    
    def __init__(self, file_path="", group_ID_column=-1, auto_open=False,
                delimiter="", enclosers=[], header_params=[],
                 keep_enclosers=True):
        """
        Creates a Subgrouped File Reader object. The filepath will be tested if
        a filepath is supplied.
        """
        Table_Reader.__init__(self, file_path, auto_open, delimiter,
                enclosers, header_params, keep_enclosers)
        if group_ID_column != -1:
            self.Set_Group_ID_Column_No(group_ID_column)
        else: 
            self.current_group_ID = ""
            self.next_row = []
    
    
    
    # Property Methods #########################################################
    
    def Set_Group_ID_Column_No(self, col_no):
        """
        Set a number as the column number which contains the group ID.
        
        Uses a 0-index system. (The first column is column 0)
        """
        try:
            col_no = int(col_no)
            if col_no < -1: 1/0
        except:
            self.group_ID_column = -1
            self.printE(self._MSG__invalid_col_group_ID)
            return
        self.group_ID_column = col_no

    def Get_Group_ID_Column_No(self):
        """
        Return the column number which contains the group ID.
        """
        return self.group_ID_column
    
    
    
    # File I/O Methods #########################################################
    
    def Open(self, new_path=""):
        """
        Attempts to open a file. If a file path is not specified, the stored
        file path will be used instead.
        
        Requires at least one delimiter to be set.
        """
        if self.group_ID_column == -1:
            self.printE(self._MSG__invalid_col_group_ID)
        if self.delimiter:
            File_Reader.Open(self, new_path)
        else:
            self.printE(self._MSG__no_delimiter)
            return
    
    def Copy_Element(self, element):
        """
        Return a copy of the current values, sanitized.
        """
        copy = []
        for i in element:
            copy.append(list(i))
        return copy
    
    def Get_Size(self):
        """
        Return the number of different groups in the table.
        """
        if ((self.file_path) and (self.group_ID_column != -1) and
                (self.delimiter)):
            count = 0
            f = open(self.file_path, "U")
            line = f.readline()
            if self.header_params:
                for param in self.header_params:
                    if type(param) == int:
                        while param > 0:
                            line = f.readline()
                            param -= 1
                    if type(param) == str:
                        while line.find(param) == 0:
                            line = f.readline()
            current_ID = ""
            while line:
                values = self._process_raw(line, self.delimiter, self.enclosers,
                        self.keep_enclosers)
                ID = values[self.group_ID_column]
                if ID != current_ID:
                    current_ID = ID
                    count += 1
                line = f.readline()
            f.close()
            return count
        return -1
        
        
        
    # File Reading Methods #####################################################

    def Read_Header(self):
        """
        Read in the header rows of the file and store them separately according
        to the params specified.
        
        [params] is expected to be a list of integers and/or strings. For
        integers, that many rows will be added directly to the "header_text"
        variable. For strings, rows will be added directly to the "header_text"
        variable as long as those rows begin with the string specified.
        """
        params = self.header_params
        sb = ""
        line = self.file.readline()
        for param in params:
            if type(param) == int:
                while param > 0:
                    sb += line
                    line = self.file.readline()
                    param -= 1
            if type(param) == str:
                while line.find(param) == 0:
                    sb += line
                    line = self.file.readline()
        self.header_text = sb
        #
        values = self._process_raw(line, self.delimiter, self.enclosers,
                self.keep_enclosers)
        self.next_row = values
        self.current_raw = self.file.readline()
    
    def _get_next_element(self):
        """
        Read in the next rows and process them.
        
        Return an empty string if the end of the file has been reached.
        """
        # Next subgroup
        row = self.next_row
        if row == [""]: return [[""]]
        group_ID = row[self.group_ID_column]
        result = [list(row)]
        # Read on, loop
        flag = True
        while flag:
            # Read
            if self.enclosers:
                values = self._process_raw(self.current_raw, self.delimiter,
                        self.enclosers, self.keep_enclosers)
            else:
                values = self._process_raw__SIMPLE(self.current_raw,
                        self.delimiter)
            self.next_row = values
            # Check for EOF
            if values and values != [""]:
                ID = values[self.group_ID_column]
            else:
                ID = None
            # Check for new section
            if ID != group_ID:
                flag = False
            else:
                result.append(values)
            # Next
            self.current_raw = self.file.readline()
        # Return
        return result


