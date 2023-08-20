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



# Enums ########################################################################

class METHOD:
    NONE=0
    TAG=1



# Lists ########################################################################

LIST__tag = ["TAG", "Tag", "tag", "T", "t"]

try:
    test = LIST__newline
except:
    LIST__newline = ["\n", "\r", "\n\r", "\r\n"]
    # Should already exist in imported modules



# Dictionaries #################################################################

DICT__grouping_methods = {
    0: "N/A",
    1: "TAG"}



# Classes ######################################################################

class GTF_Reader(Table_Reader):
    """
    The GTF Reader is a file reader designed specifically to faciliate working
    with GTF files.
    
    The main convenience it offers it grouping consecutive entries by tags found
    in the 9th column and returning them as a single object.
    
    It also for each row, a list of 9 strings and 1 dictionary will be stored
    and returned to the user when the user calls the current element. The 9
    strings correspond to the values in the 9 columns of the GTF file, while the
    dictionary is an easily accessible way to get information on the tags.
    
    Additional functionality may be added in the future.
    
    Designed for the following use:
    
    f = GTF_Reader()
    f.Set_New_Path("F:/Filepath.gtf")
    f.Autodetect_Delimiter() # OR f.Set_Delimiter(",")
    f.Set_Method("TAG")
    f.Set_Tag("gene_id")
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
    
    empty_element = [[]]
    
    # Minor Configurations #####################################################
    
    _CONFIG__print_errors = True
    _CONFIG__print_progress = False
    _CONFIG__print_metrics = True
    
    
    
    # Strings ##################################################################
    
    _MSG__object_type = "GTF File Reader"
    
    _MSG__invalid_group_type = "ERROR: Invalid grouping method. Please specify"\
            " one of the following:\n\tTAG"
    
    _MSG__no_tag = "ERROR: No tag specified."
    
    _MSG__abnormal = "Abnormal data detected:\n\t{s}"
    
    _MSG__unknown_ID_type = "SOFTWARE IMPLEMENTATION ERROR:\n\t"\
            "Attempted to use unknown ID type.\n\t"\
            "Please contact the developer."
    
    
    
    # Constructor & Destructor #################################################
    
    def __init__(self, file_path="", auto_open=False, header_params=[],
                grouping_method="TAG", tag=""):
        """
        Creates a Subgrouped File Reader object. The filepath will be tested if
        a filepath is supplied.
        """
        Table_Reader.__init__(self, file_path, auto_open, "\t", [],
                header_params, True)
        self.grouping = METHOD.NONE
        self.tag = tag
        self.Set_Current_ID(None)
        self.Set_Next_ID(None)
        if grouping_method:
            self.Set_Grouping_Method(grouping_method)
    
    
    
    # Property Methods #########################################################
    
    def Set_Grouping_Method(self, grouping_method_str):
        """
        Set the grouping method the reader is to use.
        """
        if grouping_method_str in LIST__tag:
            self.grouping = METHOD.TAG
        else:
            self.printE(self._MSG__invalid_group_type)
            return
    
    def Get_Grouping_Method(self):
        """
        Return a string representation of the grouping method, which can also be
        used to set the grouping method.
        """
        return DICT__grouping_methods[self.grouping]
    
    def Set_Tag(self, tag_str):
        """
        Set the tag to use for grouping.
        """
        self.tag = tag_str
    
    def Get_Tag(self):
        """
        Return the tag to use for grouping.
        """
        return self.tag
    
    def Set_Current_ID(self, ID):
        """
        Set the current group ID.
        """
        type_ = type(ID)
        if ID == None:
            self.current_ID = None
        elif type_ == str:
            self.current_ID = ID
        elif type_ == tuple:
            self.current_ID = tuple(ID)
        elif type_ == list:
            self.current_ID = list(ID)
        else:
            # This should never trigger unless a future developer messes up.
            print "s"
            raise Exception(self._MSG__unknown_ID_type)
    
    def Get_Current_ID(self):
        """
        Get a copy of the current group ID.
        """
        type_ = type(self.current_ID)        
        if self.current_ID == None:
            return None
        elif type_ == str:
            return self.current_ID
        elif type_ == tuple:
            return tuple(self.current_ID)
        elif type_ == list:
            return list(self.current_ID)
    
    def Set_Next_ID(self, ID):
        """
        Set the next group ID.
        """
        type_ = type(ID)
        if ID == None:
            self.next_ID = None
        elif type_ == str:
            self.next_ID = ID
        elif type_ == tuple:
            self.next_ID = tuple(ID)
        elif type_ == list:
            self.next_ID = list(ID)
        else:
            # This should never trigger unless a future developer messes up.
            raise Exception(self._MSG__unknown_ID_type)
    
    def Get_Next_ID(self):
        """
        Get a copy of the next group ID.
        """
        type_ = type(self.next_ID)
        if self.next_ID == None:
            return None
        elif type_ == str:
            return self.next_ID
        elif type_ == tuple:
            return tuple(self.next_ID)
        elif type_ == list:
            return list(self.next_ID)
    
    def Push_Next_ID(self, ID):
        """
        Slide the "next" ID to become the current ID, and set a new "next" ID.
        """
        next_ID = self.Get_Next_ID()
        self.Set_Current_ID(next_ID)
        self.Set_Next_ID(ID)
    
    
    
    # File I/O Methods #########################################################
    
    def Open(self, new_path=""):
        """
        Attempts to open a file. If a file path is not specified, the stored
        file path will be used instead.
        
        Requires at least one delimiter to be set.
        """
        if self.grouping == METHOD.NONE:
            self.printE(self._MSG__invalid_group_type)
            return
        if self.grouping == METHOD.TAG:
            if not self.tag:
                self.printE(self._MSG__no_tag)
                return
        File_Reader.Open(self, new_path)
    
    def Copy_Element(self, element):
        """
        Return a copy of the current values, sanitized.
        """
        copy = []
        for row in element:
            temp = []
            for i in row:
                if type(i) == str:
                    temp.append(i)
                if type(i) == dict:
                    temp.append(dict(i))
            copy.append(temp)
        return copy
    
    def Get_Size(self):
        """
        Return the number of lines of data in the file, excluding headers.
        """
        if self.file_path:
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
                count += 1
                line = f.readline()
            f.close()
            return count
        return -1
    
    def __new(self):
        """
        Reset the state indicators when a new file is opened.
        """
        File_Reader.__new()
        self.Set_Current_ID(None)
        self.Set_Next_ID(None)
    
    
    
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
        values = self._process_raw(line)
        self.next_row = values
        self.current_raw = self.file.readline()
    
    def _get_next_element(self):
        """
        Read in the next rows and process them.
        
        Return an empty string if the end of the file has been reached.
        """
        # Next subgroup
        row = self.next_row
        if row == [""]: return self.empty_element
        group_ID = self.Get_Next_ID()
        result = [list(row)]
        # Read on, loop
        flag = True
        while flag:
            # Read
            values = self._process_raw(self.current_raw)
            self.next_row = values
            # Check for EOF
            if values and values != [""]:
                ID = self._get_group_ID(values)
            else:
                ID = None
            # Check for new section
            if ID != group_ID:
                flag = False
            else:
                result.append(values)
            # Next
            self.Push_Next_ID(ID)
            self.current_raw = self.file.readline()
        # Return
        return result
    
    def _process_raw(self, raw_str):
        """
        Process a line of raw text from the GTF file into a list of strings for
        each of the columns and a dictionary of all the tag:value pairs in the
        9th column. The raw text from the 9th column is also included in the
        list of strings.
        """
        results = Table_Reader._process_raw__SIMPLE(self, raw_str, "\t")
        if results == [""]: return results
        if len(results) < 9:
            self.printE(self._MSG__abnormal.format(s = raw_str))
            return [""]
        tags_dict = self._parse_tags(results[8])
        results.append(tags_dict)
        return results
        
    def _parse_tags(self, raw_str):
        """
        Parse the raw text from the tags column (9th column) and return the
        information as a field:value dictionary.
        """
        # Setup
        results = {}
        # Parse
        pairs = raw_str.split(";")
        while "" in pairs: pairs.remove("") # CAN BE OPTIMIZED
        if not pairs: return results
        if pairs[-1][-1] in LIST__newline: pairs[-1] = pairs[-1][:-1]
        for pair in pairs:
            values = pair.split(" ")
            while "" in values: values.remove("") # CAN BE OPTIMIZED
            if len(values) > 1:
                key, value = values
                value = value.strip("\"")
                results[key] = value
        # Return
        return results
    
    def _get_group_ID(self, values):
        """
        Return a unique, non-pointer ID for a set of values. The values are
        assumed to have been derived from a GTF using this class's parser, and
        should consist of a list of 9 strings and 1 dictionary.
        
        The method for getting the group ID will depend on the GTF Reader
        settings.
        """
        if self.grouping == METHOD.TAG:
            return values[-1].get(self.tag, None)
        raise Exception("CRITICAL ERROR: File reading somehow commenced "\
                "without a grouping method set.")
    
    def _process_GTF_file(self, raw_str):
        """
        Process a line of data from a GTF file and return it as a list of 9
        strings and a dictionary.
        """
        values = raw_str.split("\t")
        if values[-1][-1] == "\n": values[-1] = values[-1][:-1]
        temp1 = values[-1].split(";")
        temp2 = []
        for i in temp1:
            temp2.append(i.strip(" "))
        dict_ = {}
        for i in temp2:
            key, value = i.split(" ")
            if value[0] == "\"": value = value[1:]
            if value[-1] == "\"": value = value[:-1]
            dict_[key] = value
            values.append(dict_)
        return values


