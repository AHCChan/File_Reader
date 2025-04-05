"""
MULTI-TRACK BED FILE READING COORDINATOR
(version 1.0)
by Angelo Chan

This module contains a Class capable of reading multiple BED files.

This module is designed to function as a go-between which simplifies the writing
of code which handles data from multiple BED files. Instead of having to write
the code logic which coordinates the reading and parsing of multiple BED files,
programmers can simply call a class method of this reader.

This module uses one BED file (such as one file containing loci or segment bin
data) as an "anchor", while the other files, presumably much larger and
containing data (such as aligned sequencing data) will be parsed and stored in
buffers for the user to access and use.

Requires:
    1x Anchoring file (BED file)
        The coordinates of the loci of interest.
    1x Chromosome order file (TSV file or any Single-Column file)
        The order in which chromosomes appear.
        A chromosome sizes file can be used.
    1+ Data Files (BED file)
        Contains the data of interest.

At each "Read()" command, the reader will read in the next locus in the loci
file and then, for each data file, read until they are past that locus, with all
data stored in a buffer.

Depending on the settings, data entries from before the locus might be stored in
a buffer, data entries from the previous chromosomes might be stored in a buffer
and data entries which partially but not fully overlap with the locus might be
stored in a buffer.



INCOMPLETE/BUGGY:

When reading "spanning" data, that is to say, sections which span multiple loci
of interest, the spanning data is not properly sorted into partial overlaps
on the upstream side, partial overlaps on the downstream side, and fully
overlapping data points. This is only a problem if precisely distinguishing
these things is important, which is presumed to generally not be the case.
(Assumed scenario is for ANY partial overlap) This problem can be corrected in
post.

The code logic for Read_Final() is also a bit shaky and should be checked before
serious usage.
"""

# Imported Modules #############################################################

from File_Reader import * #1.2



# Lists ########################################################################



# Dictionaries #################################################################



# Classes ######################################################################

class Multitrack_BED_Coordinator(File_Reader):
    """
    The BED File Reader is a file reader designed specifically to work with BED
    files.
    
    In addition to the standard option of reading entries in one by one, it can
    also store multiple entries in a buffer, and also read 
    
    Designed for the following use:
    
    mbc = Multitrack_BED_Coordinator()
    mbc.Set_Retain_Partial_Overlaps(True) # Default is True
    mbc.Set_Retain_Prior(True)            # Default is False
    mbc.Set_Retain_Skipped_Chrs(True)    # Default is False
    mbc.Set_Retain_Remainder(True)        # Default is False
    mbc.Set_Retain_Remaining_Chrs(True)   # Default is False
    mbc.Set_Anchor_Path("F:/Gene_Coords.bed")
    mbc.Add_Data_Path("F:/WT_Ctrl.bed") # Automatically assigned IDs based on
    mbc.Add_Data_Path("F:/WT_Drug.bed") # the order they were added. To access
    mbc.Add_Data_Path("F:/MT_Ctrl.bed") # the data for WT_Ctrl, the ID number
    mbc.Add_Data_Path("F:/MT_Drug.bed") # needed is 0, etc.
    
    mbc.Get_Chromosome_Order("F:/chr_sizes.tsv")
    # Alternatievely, mar.Set_Chromosome_Order(["chr1", "chr2", "chr3"])
    
    while not mbc.EOF:
        mbc.Read()
        # <- Code for dealing with remaining data from previous chromosome, if
                applicable
        # <- Code for dealing with data from skipped chromosomes, if applicable
        # <- Code for dealing with data from the current chromosome, but before
        #       the current loci
        # <- Code for dealing with data which overlaps with current loci
    # <- Code for dealing with remaining data from last chromosome entry
    # <- Code for dealing with the data from all the other untouched chromosomes
    """
    
    # Data Structures ##########################################################
    
    empty_element = []
    placeholder_coords = ["", -1, -1]
    
    
       
    # Minor Configurations #####################################################
    
    _CONFIG__print_errors = True
    _CONFIG__print_progress = False
    _CONFIG__print_metrics = True
    
    
    
    # Defaults #################################################################
    
    _DEFAULT__rt_partial        = True
    _DEFAULT__rt_prior          = False
    _DEFAULT__rt_skipped_chrs   = False
    _DEFAULT__rt_remainder      = False
    _DEFAULT__rt_remaining_chrs = False
    
    
    
    # Message Strings (Inherited) ##############################################
    
    _MSG__object_type = "Multitrack BED File Reading Coordinator"
    _MSG__units_of_measure = "Loci"
    
    _MSG__invalid_chr = "ERROR: Chromosome \"{S}\" is not the specified list of"\
            " chromosomes.\n\nFile reader will now close to prevent a repeat "\
            "this error message."
    _MSG__invalid_coord_num = "ERROR: Invalid genomic location.\n"\
            "\tFile:   {f}\n"\
            "\tRow:    {r}\n"\
            "\tString: {s}\n\n"\
            "File reader will now close to prevent a repeat of this error"\
            "message."
    
    _MSG__init_message = "Preparing Multitrack BED Coordinator..."
    
    
    
    # Message Strings (New) ####################################################
    
    _MSG__init_message = "Preparing Multitrack BED Coordinator..."
    
    _MSG__no_path_loci = "ERROR: No filepath specified for the locus file.\n"
    _MSG__no_path_chrs = "ERROR: No filepath specified for the chromosome "\
            "order file.\n       Also, no chromosome order was specified.\n"
    _MSG__no_path_data = "ERROR: No filepaths specified for data files.\n"
    
    _MSG__open_commence = "Attempting to open files...\n"
    _MSG__open_success = "All files successfully opened."
    
    _MSG__open_loci_fail = "ERROR: Unable to open locus file.\n"
    _MSG__open_chrs_fail = "ERROR: Unable to open chromosomes order file.\n"
    _MSG__open_data_fail = "ERROR: Unable to open the data file(s):\n\t{PATH}\n"
    
    _MSG__coords_too_short = "ERROR: Not enough values for genomic "\
            "coordinates.\n\nOccured on line no {LINE}.\n"
    _MSG__invalid_chr = "ERROR: Invalid chromosome name: {STRING}"\
            "\n\nOccured on line no {LINE}.\n"
    _MSG__invalid_chr = "ERROR: Invalid genomic coordinate: {STRING}"\
            "\n\nOccured on line no {LINE}.\n"
    
    
    
    # Constructor & Destructor #################################################
    
    def __init__(self):
        """
        Creates a Multitrack BED Coordinator object.
        Default settings will be used and need to be set manually.
        """
        self.printP(self._MSG__init_message)
        self.file_opened = False
        self.EOF = True
        self.chr_order = []
        self.Reset_Settings()
        self.Reset_Paths()
        self.files_data = []
        self.Reset_Data()
    
    
    
    # Property Methods #########################################################
    
    def __str__(self):
        """
        Return a string representation of the currently selected file.
        """
        sb = ("<{T} Object> - ".format(T = self._MSG__object_type))
        if self.chr_order:
            sb += "\n\tCHR ORDER:      YES"
        else:
            sb += "\n\tCHR ORDER:      NO"
        if self.path_chrs:
            sb += "\n\tCHR ORDER FILE: {P}".format(P = self.path_chrs)
        else:
            sb += "\n\tCHR ORDER FILE: -"
        if self.path_loci:
            sb += "\n\tLOCI FILE:      {P}".format(P = self.path_loci)
            sb += "\n\t                {S} {U}".format(S = self.Get_Size(),
                    U = self._MSG__units_of_measure)
        else:
            sb += "\n\tLOCI FILE:      -"
        if self.paths_data:
            sb += "\n\tDATA FILE(s):   {P}".format(
                    P = ("\n\t" + 16*" ").join(self.paths_data))
        else:
            sb += "\n\tDATA FILE(s):   -"
        return sb
    
    def Get(self):
        """
        Get a deep copy of the data from the current locus, including partials
        if the coordinator was set to permit them.
        """
        result = []
        for i in self.indexes:
            temp = []
            for values in self.current_before[i]:
                temp.append(list(values))
            for values in self.current[i]:
                temp.append(list(values))
            for values in self.current_after[i]:
                temp.append(list(values))
            result.append(temp)
        return result
    
    def Get_Size(self):
        """
        Return the number of lines in the locus file. Assuming no headers, this
        should be the number of loci.
        Return -1 if there is no locus file.
        Return -2 if an anchoring file has been set but it can't be opened.
        """
        if not self.path_loci: return -1
        try:
            f = open(self._path__anchor, "U")
        except:
            return 2
        count = 0
        for line in f: count += 1
        f.close()
        return count
    
    def Copy_Element(self, element):
        """
        Return a copy of [element] which can be modified without affecting the
        original.
        """
        return list(element)
    
    def Reset_Settings(self):
        """
        Reset all settings to their default.
        """
        self.retain_partial = True
        self.retain_prior = False
        self.retain_skipped = False
        self.retain_remainder = False
        self.retain_remaining_chrs = False
    
    def Set_Retain_Partial_Overlaps(self, boolean):
        """ Standard parameter setter. """
        self.retain_partial = boolean
        
    def Set_Retain_Prior(self, boolean):
        """ Standard parameter setter. """
        self.retain_prior = boolean
    
    def Set_Retain_Skipped(self, boolean):
        """ Standard parameter setter. """
        self.retain_skipped = boolean
    
    def Set_Retain_Remainder(self, boolean):
        """ Standard parameter setter. """
        self.retain_remainder = boolean
    
    def Set_Retain_Remaining_Chrs(self, boolean):
        """ Standard parameter setter. """
        self.retain_remaining_chrs = boolean
    
    def Get_Retain_Partial_Overlaps(self):
        """ Standard parameter getter. """
        return self.retain_partial
        
    def Get_Retain_Prior(self):
        """ Standard parameter getter. """
        return self.retain_prior
    
    def Get_Retain_Skipped(self):
        """ Standard parameter getter. """
        return self.retain_skipped
    
    def Get_Retain_Remainder(self):
        """ Standard parameter getter. """
        return self.retain_remainder
    
    def Get_Retain_Remaining_Chrs(self):
        """ Standard parameter getter. """
        return self.retain_remaining_chrs
        
    def Get_Chr_Order(self):
        """
        Return the current list of chromosomes, in order.
        """
        return self.Copy_Element(self.chr_order)
    
    def Set_Chr_Order(self, list_of_chrs):
        """
        Set the chromosome order by providing a list.
        """
        self.chr_order = list(list_of_chrs)
    
    def Get_Cur_Locus_Data(self):
        """
        Return the current locus data.
        """
        return self.current_locus_data
    
    def Set_Cur_Data(self, list_):
        """
        Set the current locus data.
        """
        self.current_locus_data = list(list_)
    
    def Get_Cur_Coords(self):
        """
        Return the genomic coordinates of the current locus.
        """
        return self.current_coords
    
    def Set_Cur_Coords(self, list_):
        """
        Update the genomic coordinates of the current locus.
        """
        self.cur_chr = list_[0]
        self.cur_start = list_[1]
        self.cur_end = list_[2]
    
    def Get_Cur_Chr(self):
        """
        Return the chromosome name of the current entry.
        """
        return self.cur_chr
    
    def Get_Cur_Start(self):
        """
        Return the genomic location of the start of the current locus.
        """
        return self.cur_start
    
    def Get_Cur_End(self):
        """
        Return the genomic location of the end of the current locus.
        """
        return self.cur_end
    
    def Get_Cur_Size(self):
        """
        Return the size of the current entry.
        """
        size = self.cur_end - self.cur_start + 1
        return size

    def Get_Data_Prev_Chrs(self):
        """
        Get a deep copy of the data from previous, untouched chromosomes.
        """
        result = []
        for d in self.prev_chrs:
            temp_1 = {}
            for k in d.keys():
                temp_2 = []
                values = d[k]
                for sublist in values:
                    temp_3 = list(sublist)
                    temp_2.append(temp_3)
                temp_1[k] = temp_2
            result.append(temp_1)
        return result

    def Get_Data_Prior(self):
        """
        Get a deep copy of the data from before the current locus, on the same
        chromosome.
        """
        result = []
        for list_ in self.prior:
            result.append(list(list_))
        return result

    def Get_Data_Current_Before(self):
        """
        Get a deep copy of the data from the upstream partial overlaps with the
        current locus.
        """
        result = []
        for list_ in self.current_before:
            result.append(list(list_))
        return result

    def Get_Data_Current(self):
        """
        Get a deep copy of the data from the current locus.
        """
        result = []
        for list_ in self.current:
            result.append(list(list_))
        return result

    def Get_Data_Current(self):
        """
        Get a deep copy of the data from the downstream partial overlaps with
        the current locus.
        """
        result = []
        for list_ in self.current_after:
            result.append(list(list_))
        return result

    def Get_Data_Remainder(self):
        """
        Get a deep copy of the data from the last touched chromosome, after the
        final locus of that chromosome.
        """
        result = []
        for list_ in self.remainder:
            result.append(list(list_))
        return result

    def Get_Data_Final(self):
        """
        Get a deep copy of the data from any remaining untouched chromosomes.
        """
        result = []
        for d in self.final:
            temp_1 = {}
            for k in d.keys():
                temp_2 = []
                values = d[k]
                for sublist in values:
                    temp_3 = list(sublist)
                    temp_2.append(temp_3)
                temp_1[k] = temp_2
            result.append(temp_1)
        return result
    
    
    
    # File Path Methods ########################################################
    
    def Set_New_Path(self, new_path=""):
        """ Invalid inherited method. """
        print(self._MSG__method_should_not_call)
    
    def Reset_Paths(self):
        """ Reset all filepaths. """
        self.path_loci = ""
        self.path_chrs = ""
        self.paths_data = []
        self.indexes = []
        self.file_count = 0
    
    def Set_Path_Loci(self, new_path):
        """ Set the path for the locus file. """
        self.path_loci = new_path
    
    def Set_Path_Chrs(self, new_path):
        """ Invalid inherited method. """
        self.path_chrs = new_path
    
    def Add_Path_Data(self, new_path):
        """ Invalid inherited method. """
        self.paths_data.append(new_path)
        self.file_count += 1
    
    
    
    # File I/O Methods #########################################################
    
    def __set_path(self, file_path):
        """ Invalid inherited method. """
        print(self._MSG__method_should_not_call)
    
    def __new(self):
        """
        Reset the state indicators when a new file is opened.
        """
        self.file_opened = True
        self.EOF = False
        self.next_locus = self.Copy_Element(self.empty_element)
        self.Read()
    
    def Reset_Data(self):
        """
        Reset all file states and all data.
        """
        self.Close()
        self.Clear_Filestates()
        self.Clear_Buffers()
    
    def Open(self, new_path=""):
        """
        Commence file reading process.
        """
        # Clean up
        if self.file_opened:
            self.Close()
        # Check for filepaths
        flag = True
        if not self.path_loci:
            self.printE(self._MSG__no_path_loci)
            flag = False
        if not self.path_chrs and not self.chr_order:
            self.printE(self._MSG__no_path_chrs)
            flag = False
        if not self.paths_data:
            self.printE(self._MSG__no_paths_data)
            flag = False
        # Attempt to open
        if flag:
            self.printP(self._MSG__open_commence)
            # Locus file
            try:
                self.file_loci = open(self.path_loci, "U")
                self.Read_Raw()
            except:
                self.printE(self._MSG__open_loci_fail.format(
                        PATH = self.path_loci))
                flag = False
            if not self.next_raw: flag = False # Empty locus file
            # Chromosome order file
            if self.path_chrs:
                rt = self.Parse_Chr_Order(self.path_chrs)
                if rt:
                    self.printE(self._MSG__open_chrs_fail.format(
                        PATH = self.path_chrs))
                    flag = False # Empty chromosome order file
            else:
                self.chr_order_tmp = list(self.chr_order)
                self.chr_order_set = set(self.chr_order_tmp)
            # Data files
            temp = []
            for path in self.paths_data:
                try:
                    f = Simplified_BED_Reader(path)
                    temp.append(f)
                except:
                    self.printE(self._MSG__open_data_fail.format(PATH = f))
                    flag = False # If any data file should fail to open
            if flag:
                self.files_data = temp
            else:
                for f in temp: f.Close()
        else:
            return 1
        # Finalize
        if flag:
            self.EOF = False
            self.file_opened = True
            self.indexes = range(len(self.paths_data))
            self.printP(self._MSG__open_success)
            return 0
        else:
            return 2
    
    def Close(self):
        """
        Close the object's files if they are open. Do nothing if there are no
        files open.
        
        Note that the filestate variables and data buffers are not automatically
        cleared as well because a program may wish to close all currently opened
        files but retain the data in the buffers.
        """
        if self.file_opened:
            self.file_loci.close()
        for f in self.files_data: f.Close()
    
    def Clear_Filestates(self):
        """
        Clear the filestate variables used to orchestrate the reading process.
        """
        self.Set_Cur_Coords(self.placeholder_coords)
        self.next_raw = ""
        self.count_loci = 0
        self.last_processed_chr = ""
    
    def Clear_Buffers(self):
        """
        Clear all data buffers which store data from the data files.
        """
        self.prev_chrs = []
        self.prior = []
        self.current_before = []
        self.current = []
        self.current_after = []
        self.remainder = []
        self.final_remainder = []
        self.final_untouched = []
    
    
    
    # File Reading Methods #####################################################
    
    def Is_Empty_Element(self, element):
        """
        Return True if [element] is an "empty" element, that is to say, an empty
        string.
        
        Return False otherwise.
        """
        print(self._MSG__method_should_not_call)
    
    def Read(self):
        """
        Read in the next locus, and then read all the data files until the end
        of that locus.
        """
        rt = self.Next_Locus()
        if rt: return rt # Next_Locus returned an error code
        self.Push_Buffers()
        for i in self.indexes:
            f = self.files_data[i]
            # New chromosome
            if self.cur_chr != self.last_processed_chr:
                # Finish previous chromosome
                temp = []
                if self.retain_remainder:
                    while (f.chr == self.last_processed_chr) and (not f.EOF):
                        temp.append(f.values)
                        f.Read()
                else:
                    while (f.chr == self.last_processed_chr) and (not f.EOF):
                        f.Read()
                self.remainder[i] = (temp)
                # Skipped chromosomes
                temp = {}
                if self.retain_prior:
                    while ((self.chr_order_key[f.chr] <
                            self.chr_order_key[self.cur_chr])
                            and (not f.EOF)):
                        if f.chr not in temp:
                            temp[f.chr] = [f.values]
                        else:
                            temp[f.chr].append(f.values)
                        f.Read()
                else:
                    while ((self.chr_order_key[f.chr] <
                            self.chr_order_key[self.cur_chr])
                            and (not f.EOF)):
                        f.Read()
                self.prev_chrs[i] = (temp)
                # Push through
                self.last_processed_chr = self.cur_chr
            # Prior loci
            temp = []
            if self.retain_prior:
                while f.end < self.cur_start:
                    temp.append(f.values)
                    f.Read()
            else:
                while f.end < self.cur_start:
                    f.Read()
            self.prior[i] = (temp)
            # Overlap before
            temp = []
            if self.retain_partial:
                if self.retain_prior:
                    while f.start < self.cur_start:
                        if f.end < self.cur_start:
                            self.prior[i].append(f.values)
                        else:
                            temp.append(f.values)
                        f.Read()
                else:
                    while f.start < self.cur_start:
                        if f.end >= self.cur_start:
                            temp.append(f.values)
                        f.Read()
            else:
                while f.start < self.cur_start:
                    f.Read()
            self.current_before[i] = (temp)
            # Current
            while f.end <= self.cur_end:
                if f.start >= self.cur_start:
                    self.current[i].append(f.values)
                else:
                    if self.retain_partial:
                        self.current_before[i].append(f.values)
                f.Read()
            # Overlap after
            temp = []
            while f.start < self.cur_end:
                if self.retain_partial:
                    temp.append(f.values)
                f.Read()
            self.current_after[i] = (temp)
        self.count_loci += 1
    
    def Read_Final(self):
        """
        The end of the locus file has been reached. Read in the rest of the data
        entries.
        
        Doesn't check the retain_remaining_chrs flag.
        """
        # Clear
        self.final_remainder = []
        self.final_untouched = []
        #
        for i in self.indexes:
            temp = []
            f = self.files_data[i]
            while ((self.chr_order_key[f.chr] <
                    self.chr_order_key[self.cur_chr]) and (not f.EOF)):
                f.Read()
            while (f.chr == self.last_processed_chr) and (not f.EOF):
                temp.append(f.values)
                f.Read()
            self.final_remainder.append(temp)
            temp = {}
            while not f.EOF:
                if f.chr not in temp:
                    temp[f.chr] = [f.values]
                else:
                    temp[f.chr].append(f.values)
                f.Read()
            f.Read()
            self.final_untouched.append(temp)
    
    def Push_Buffers(self):
        """
        Push data from the overlapping buffers.
        """
        temp = self.Generate_Empty_Buffer_LIST()
        if self.cur_chr == self.last_processed_chr:
            if self.retain_partial:
                for i in self.indexes:
                    for values in self.current_before[i]:
                        if ((values[2] >= self.cur_start) or
                                (values[1] >= self.cur_end)):
                            temp[i].append(values)
                    for values in self.current[i]:
                        if ((values[2] >= self.cur_start) or
                                (values[1] >= self.cur_end)):
                            temp[i].append(values)
                    for values in self.current_after[i]:
                        if ((values[2] >= self.cur_start) or
                                (values[1] >= self.cur_end)):
                            temp[i].append(values)
        self.prev_chrs = self.Generate_Empty_Buffer_DICT()
        self.prior = self.Generate_Empty_Buffer_LIST()
        self.current_before = self.Generate_Empty_Buffer_LIST()
        self.current = temp
        self.current_after = self.Generate_Empty_Buffer_LIST()
        self.remainder = self.Generate_Empty_Buffer_LIST()
    
    def Generate_Empty_Buffer_LIST(self):
        """
        Create an empty buffer for LIST type data.
        """
        result = []
        for i in self.indexes:
            result.append([])
        return result
    
    def Generate_Empty_Buffer_DICT(self):
        """
        Create an empty buffer for DICT type data.
        """
        result = []
        for i in self.indexes:
            result.append({})
        return result
    
    def Next_Locus(self):
        """
        Read up to the next locus.
        """
        values = self.next_raw.split("\t")
        # Too short
        if len(values) < 3:
            self.printE(self._MSG__coords_too_short.format(
                    LINE = self.count_loci+1))
            return 1
        # Invalid coordinates
        flag = True
        if values[0] not in self.chr_order_set:
            self.printE(self._MSG__invalid_chr.format(
                    STRING = values[0],
                    LINE = self.count_loci+1))
            flag = False
        try:
            start = int(values[1])
        except:
            self.printE(self._MSG__invalid_coord.format(
                    STRING = values[1],
                    LINE = self.count_loci+1))
            flag = False
        try:
            end = int(values[2])
        except:
            self.printE(self._MSG__invalid_coord.format(
                    STRING = values[2],
                    LINE = self.count_loci+1))
            flag = False
        # Success
        if flag:
            self.Set_Cur_Data(values)
            self.Set_Cur_Coords([values[0], start, end])
            self.Read_Raw()
        else: # Fail
            self.EOF = True
            return 2
    
    def Read_Raw(self):
        """
        Parse in the next line of the locus file, raw, and store it in the
        buffer.
        """
        raw = self.file_loci.readline()
        if raw and raw[-1] == "\n": raw = raw[:-1]
        self.next_raw = raw
        if not raw: self.EOF = True
    
    def Parse_Chr_Order(self, filepath):
        """
        Get the chromosome order from a file. Change the chromosome order if the
        if file is valid.
        
        Return 0 if everything proceeds smoothly.
        Return 1 if the file could not be opened.
        Return 2 if the file is empty.
        """
        temp = []
        try:
            f = open(filepath, "U")
        except:
            return 1
        for line in f:
            line = line.split("\t")
            if line[-1][-1] == "\n": line[-1] = line[-1][:-1]
            chr_name = line[0]
            temp.append(chr_name)
        f.close()
        if temp:
            self.chr_order = temp
            self.chr_order_tmp = list(temp)
            self.chr_order_set = set(self.chr_order_tmp)
            self.chr_order_key = {}
            counter = 0
            for i in self.chr_order_tmp:
                self.chr_order_key[i] = counter
                counter += 1
            return 0
        return 2



class Simplified_BED_Reader():
    """
    Simplified BED reader specifically made for the Multitrack BED Reader. Only
    intended for internal use.
    """
    
    # Constructor & Destructor #################################################
    
    def __init__(self, filepath):
        """ Open a file. Throws error if unsuccessful. """
        self.file = open(filepath, "U")
        self.EOF = False
        self.line_no = 0
        self.next_raw = self.file.readline()
        if not self.next_raw: raise Exception
        self.chr = ""
        self.start = -1
        self.end = -1
    
    def __del__(self):
        """ Trigger self.Close() to tie up loose ends. """
        self.Close()
    
    # File I/O Methods #########################################################
    
    def Close(self):
        """ Close the file. Used to tying up loose ends. """
        self.file.close()
    
    def End(self):
        """ Determine the end of file has been reached or not. """
        return self.EOF
    
    def Read(self):
        """
        Read a BED entry.

        Return 0 if successful.
        Return the line number if there is a problem.
        """
        # Push through
        self.raw = self.next_raw
        self.next_raw = self.file.readline()
        if not self.raw: self.EOF = True
        # Parse
        values = self.raw.split("\t")
        if len(values) < 3: return self.line_no + 1
        if values[-1][-1] == "\n": values[-1] = values[-1][:-1]
        self.chr = values[0]
        try:
            self.start = int(values[1])
            self.end = int(values[2])
        except:
            return self.line_no + 1
        self.values = [values[0], self.start, self.end] + values[3:]
        # Success
        self.line_no += 1
    
    # Advanced File I/O Methods ################################################
    
    def Start_Reached(self, acceptable_chrs, threshold):
        """ Checks if the file has reached a particular locus yet. """
        if self.chr in acceptable_chrs and self.end >= threshold:
            return False
        return True
    
    def End_Reached(self, acceptable_chrs, threshold):
        """ Checks if the file has reached a particular locus yet. """
        if self.chr in acceptable_chrs and self.end >= threshold:
            return False
        return True
    
    def Read_Until_Start_Reached(self, acceptable_chrs, threshold):
        """ Reach until the file has reached a particular locus. """
        while ((not self.EOF) and
                (not self.Start_Reached(acceptable_chrs, threshold))):
            self.Read()
    
    def Read_Until_End_Reached(self, acceptable_chrs, threshold):
        """ Reach until the file has reached a particular locus. """
        while ((not self.EOF) and
                (not self.Start_Reached(acceptable_chrs, threshold))):
            self.Read()
    
    
    
    # Getters ##################################################################
    
    def Get_Raw(self):
        return self.raw
    
    def Get_Data(self):
        return self.values
    
    def Get_Chr(self):
        return self.chr
    
    def Get_Start(self):
        return self.start
    
    def Get_End(self):
        return self.end



# Functions ####################################################################


