#! /usr/bin/perl
# :set ts=3
#######################################################################################
#        Name: ere: Elein's Report Engine
#    Function: Given special ere(xml/html) templates and queries, generate a report in HTML
# CVS Version: 1.20
my $version="ere version 0.9/cvs v1.17, Support at www.varlena.com. Copyright 2003 (embedded) ";
#      Author: A. Elein Mustain (elein@varlena.com)
#   Copyright: A. Elein Mustain, 2002, 2003 (see below)
#######################################################################################
#
# ere is an open source program distributed by the author, A. Elein Mustain
# (elein@varlena.com).  The support site is www.varlena.com/Reports and
# it contains documentation, examples, registration information and downloads.
#
# Any redistribution of this program must carry forward the copyright conditions
# as stated here.  The term "redistribution" in this license means your application is
# distributed to more than one physical location, i.e. it is distributed to different
# street addresses.
#
# The open source license for ere permits you to use the software at no charge
# under the condition that if you use ere in an application you redistribute, the
# complete source code for your application must be available and freely redistributable
# under reasonable conditions.
#
# If you do not want to release the source code for your application, you may 
# purchase a license. For further information, please contact elein@varlena.com
#
# Registration of your copy of ere is strongly encouraged in order for
# you to get the latest updates and enhancement. Registered owners will be
# given basic support for questions about how to use the product. 
# To register ere, see http://www.varlena.com/Reports
#
# Bug reports and suggested changes are encouraged. They may be sent to 
# info@varlena.com
#
# Consultation for report development as well as PostgreSQL support is available
# through http://www.varlena.com.
#
#######################################################################################
# 
# Redistribution and use of ere, with or without modification, are permitted
# provided that the following conditions are met:
#
#   1. Redistributions of source code must retain the above copyright notice,
#      this list of conditions and the following disclaimer.
#
#   2. Redistributions in any form must be accompanied by information on how to
#      obtain complete source code for the report software and any accompanying
#      software that uses the report software. The source code must either be
#      included in the distribution or be available for no more than the cost
#      of distribution plus a nominal fee, and must be freely redistributable
#      under reasonable conditions.
#
#   3. Changes from the original source code must be marked as such and
#      changed source should not be presented as a copy of the original source.
#
# THIS SOFTWARE IS PROVIDED BY A. Elein Mustain ``AS IS'' AND ANY EXPRESS
# OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
# OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE, OR NON-INFRINGEMENT,
# ARE DISCLAIMED. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY DIRECT,
# INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
# (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
# LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED
# AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR
# TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF
# THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
#
#######################################################################################
use strict;
use HTML::Parser;
use Data::Dumper;
use Config;
use Pg;

# Reads a ere file and creates an html file

# ============== GLOBAL VARS =====================
my $report	= $ARGV[0];	# file name, no suffix
my $debug	= 0;			# debug level

# Startup
my %var =();

# Compile time
my $reportname;	# Name of report IN report
my %report;			# Report HASH -- UNUSED;
my %datastreams;	# DataStream Definitions HASH name ( %ds,...)
my %ds;				# DataStream HASH
						# = ( 'sql'	=> SQL string,
						#     'breaks'  => array of breaks column names,
						#     'targets' => array of column names,
						#     'csv'	=> Can generate csv
						# --- RUNTIME elements ---
						#     'conn'	=> $conn, db connection
						#     'currrow' => array of values of current
						#     'nextrow' => array of values of next
						#     'currbrk' => array of current break values
						#     'nextbrk' => array of next break values
my $conn;			# Database connection
my @currrow;		# values of current row: maps to target list
my @nextrow;		# values of next row: maps to target list
my @currbrk;		# value of current break: maps to break list
my @nextbrk;		# value of next break: maps to break list
my $dsname;			# DataStream name
my $dbname;			# DataStream connection info
my $dbport;			# DataStream  connection info
my $dbuser;			# DataStream connection info
my $dboptions;		# DataStream connection info
my $dbpasswd;		# DataStream connection info
my $dbhost;			# DataStream connection info
my $dbconnstr;		# DataStream connection string
my $csv;				# CSV file, if defined
my $sql		= "";	# SQL Stream: Cumulative string
my @breaks	= ();	# Break List array of column names
my @targets	= ();	# Target List array of column names


my $tmpsql;			# compile time buffers
my $tmpbreaks	= "";
my $tmptargets	= "";

my @areas		= ();	# ARRAY of Area Arrays;
							# centralized list, used for debugging
							# Area Arrays are embedded in @HTML
							# ---------AREA Elements -------------
my $areaname;			# Area Name
my @areadata;			# DataStream Name of Area
my $areamaxr;			# maximum number of repetitions
my $areabreaks;		# Break List for Area
my $arealine;			# HTML Index for start of Area
my $areaend;			# HTML Index for END of Area
my $areacheck;			# fetch next row before beginning area
							# (used for sibling areas with maxrepeat set
							# to avoid duplicating the last row in sibling 1
							# in the first row of sibling 2)

my $level = 0;			# Level HTML tree. Not unique.
my @HTML = ();			# Array of Tuples of compiled HTML lines
							# (level, text, [optional area object]
my $column;				# Column being compiled.
my $htmlidx = 0;		# HMTL index of current line
my @openareads = ();	# DataStream names of areas currently open (at
							# compile time). Add open datastream names
							# to newly opened datastreams so that they
							# inherit the target lists. TEST STRUCTURE

# RUNTIME
my @rtds = ();			# Array of liveds: RUNTIME
my %liveds;				# Live DataStream: RUNTIME

# ============== START OF PARSER CALLBACKS =====================
sub start
{
	my($self, $tagname, $attr, $origtext) = @_;
	#
	# Report Definition
	#
	if ($tagname eq "reportdefinition"){
		return; # skip it
	}
	elsif ($tagname eq "reportname"){
		$self->handler(text => sub { $reportname = shift; }, "dtext");
	}
	#
	# Area Definitions
	#
	elsif ($tagname eq "table" || $tagname eq "area"){

		# if we are just a table and not an area inc the level, but
		# let the area act like any other line.
		@areadata	= ();
		$areaend		= 0;
		$areacheck	= 0;
		$level		= $level + 1;

		if( !$attr->{'name'} ) {
					printit ( $origtext );
					return;
		}

		if ($tagname eq "area" ) { $origtext = "<!--".$origtext."-->";}
		$areaname	= $attr->{'name'};
		$areacheck	= $attr->{'checkbreak'};	if( !$areacheck )		{ $areacheck="no";}
		$areabreaks = $attr->{'break'};			if( !$areabreaks )	{ $areabreaks = "NONE" ;}
		$areamaxr	= $attr->{'maxrepeat'};		if( !$areamaxr )		{ $areamaxr = -1 ;}

		my $tmp	=  $attr->{'datastream'};
		$tmp =~ s/"//g ;
		push @areadata,  split ' ', $tmp;
		# check datastream names
		for (my $dsi=0 ; $dsi < @areadata; $dsi++){
			my $checkdsname = $areadata[$dsi];
			if ( !$datastreams{$checkdsname} ) { 
				err(2, "Bad datastream name in area or table '$areaname'. ");
			}
		}
		#printdbg(2, "before push openareads: @openareads ");
		#push @openareads,  split ' ', $tmp;
		#printdbg(2, "after push openareads: @openareads ");
		push @HTML, [ $level, $htmlidx, $origtext,
			[$areaname, [@areadata], $areamaxr, $areabreaks, $htmlidx, $areaend, $areacheck] ];

		printdbg (3,"pushHTML w/area: $level, $htmlidx, $origtext + $areaname, @areadata, $areamaxr, $areabreaks, $htmlidx, $areaend, $areacheck");
		#print DEBUG Dumper($HTML[$htmlidx]);


		$htmlidx = $htmlidx + 1;
	}
	#
	# DataStream Definition
	#
	elsif ($tagname eq "datastreamdef"){
		if( !defined $reportname ) {
			printdbg(1, "Missing Report Name Definition.");
			err(1, "Missing Report Name Definition.");
		}
		my $val;
		$dsname    = $attr->{'name'};
		$csv       = $attr->{'csv'}; # can do csv# should do csv
		$dbname    = $attr->{'dbname'};	 # DB Connection information
		$dbport    = $attr->{'port'};		 # 
		$dboptions = $attr->{'options'};	 # 
		$dbuser    = $attr->{'user'};		 #
		$dbpasswd  = $attr->{'password'}; # 
		$dbhost    = $attr->{'host'}; # 
		printdbg(1, "Start DataStream Def: $dsname" );

		if ( $csv eq "yes" and defined $var{'do_csv'} and $var{'do_csv'} eq "t" ) { 
			printdbg(1, "\tdatastream $dsname should do: $csv ");
			my $rand1    = rand( 2 ** (8 * $Config{longsize}) );
			my $randfile = sprintf( "%s-%x.csv",$reportname,$rand1 );
			$randfile    =~ s/ /_/g;

			if ( defined $var{csvdir} ) {
				$randfile = sprintf( "%s/%s", $var{'csvdir'},$randfile );
			}
			$var{$dsname} = $randfile; # save csv file datastream named variable.
			printdbg(1, "\tdatastream $dsname csv filename: $var{$dsname} ");
		}
		$dbconnstr = "";
		if ( $dbname ne "" ) {
			$val = subvars($dbname." ");
			$dbconnstr = $dbconnstr . " dbname=" . $val;
		}
		if ( $dbport ne ""  ) {
			$val = subvars($dbport." ");
			$dbconnstr = $dbconnstr . " port=" . $val;
		}
		if ( $dboptions ne ""  ) { 
			$val = subvars($dboptions." ");
			$dbconnstr = $dbconnstr . " options=" . $val;
		}
		if ( $dbuser ne ""  ) { 
			$val = subvars($dbuser." ");
			$dbconnstr = $dbconnstr . " user=" . $val;
		}
		if ( $dbpasswd ne ""  ) { 
			$val = subvars($dbpasswd." ");
			$dbconnstr = $dbconnstr . " password=" . $val;
		}
		if ( $dbhost ne ""  ) { 
			$val = subvars($dbhost." ");
			$dbconnstr = $dbconnstr . " host=" . $val;
		}
		printdbg(1, "\tConnection String: $dbconnstr ");
		##$dbconnstr = subvars( $dbconnstr );
		#while( $dbconnstr =~ /:([a-z_\.]+):[\W ]+/g ) {
			#$column = $1;
			#if ( $var{$column} ){
				#$dbconnstr =~ s/:$column:/$var{$column}/;
				#printdbg(2,"Substituting variable: $column with $var{$column}");
			#}
			#else {
				#$dbconnstr =~ s/:$column:/ /;
				#printdbg(2,"Substituting variable: $column with nothing");
			#}
		#}

	}
	elsif ($tagname eq "breaklist"){
		$self->handler(text => sub { 
			$tmpbreaks = shift;
			$tmpbreaks =~ s/[ \t\n\r]//g;
			if (@breaks) { push @breaks , split ',',  $tmpbreaks; }
			else { @breaks = split ',',  "NONE," . $tmpbreaks; }
		}, "dtext");
	}
	# Substituting Arguments into Variables
	elsif ($tagname eq "var"){
		my $tmpname = $attr->{'name'}; 
		my $tmpval = $attr->{'value'}; 
		printdbg(4,"Variable/Argument Substituting: $tmpname($tmpval) ");
		while( $tmpval =~  /:([a-z_\.]+):[\W ]+/ ) {
				my $subvar = $1;
				printdbg(2,"Argument Substituting: ($subvar) ");
				if ( $var{$subvar} ){
					$tmpval =~ s /:$subvar:/$var{$subvar}/;
					printdbg(2,"Substituting variable: $subvar with $var{$subvar}");
				}
				else {
					$tmpval = ''; last;
					printdbg(2,"Substituting variable: $subvar with nothing");
				}
		}
		printdbg(2,"VAR: ($tmpname = $tmpval)");
		$var{$tmpname} = $tmpval;
	}
	elsif ($tagname eq "targetlist"){
		$self->handler(text => sub { 
			$tmptargets = shift;
			$tmptargets =~ s/[ 	\n\r]//g;
			if (@targets) { push @targets , split ',',  $tmptargets; }
			else { @targets = split ',',  $tmptargets; }
		}, "dtext");
	}
	elsif ($tagname eq "sql"){
		#
		# SQL String and ARG substitution
		#
		# If  :var1: contains  :var2: 
		#	e.g. <VAR name=proj value="j.project_tag = ':projarg: '" /VAR>
		# and the variable inside (:projarg: ) doesn't exists, 
		# clear out the variable encapsulating variable: (:proj:). 
		# This is an SQL only feature.
		# 
		$self->handler(text => sub { 
			$tmpsql = shift;
			$tmpsql =~ s/$/ /;

			printdbg(5,"\tSQL BEFORE Substituting: \n\t[$tmpsql]");
			$tmpsql = subvars( $tmpsql );
			#while( $tmpsql =~ /:([a-z_\.]+):[\W ]+/ ) {
				#my $tmpname = $1;
				#printdbg(4,"SQL BEFORE Substituting: name $tmpname");
				#if ( $var{$tmpname} ){
					#$tmpsql =~ s/:$tmpname:/$var{$tmpname}/;
					#printdbg(2,"SQL Substituting: $tmpname w/$var{$tmpname}");
				#}
				#else {
					#$tmpsql =~ s/:$tmpname:/ /;
					#printdbg(2,"SQL Substituting: $tmpname w/Nothing");
				#}
			#}

			$tmpsql =~ s/[\s]+/ /g;
			$tmpsql =~ s/^\s*//;
			$tmpsql =~ s/ *$//;
			$sql    =  $sql . $tmpsql." "; 
			printdbg(5,"\tSQL AFTER Substituting: \n\t[$sql]");
		}, "dtext");
	}
	#
	# Nothing of interest
	#
	else { 
		printit ( $origtext );
	}
	return;
}
sub end
{
	my($self, $tagname, $origtext) = @_;
	# Report Definition
	if ($tagname eq "reportdefinition"){
		return; # skip it
	}
	elsif ($tagname eq "reportname"){
		$self->handler(text => undef);
	}
	elsif ($tagname eq "sql"){
		$self->handler(text => undef);
	}
	elsif ($tagname eq "breaklist"){
		$self->handler(text => undef);
	}
	elsif ($tagname eq "targetlist"){
		$self->handler(text => undef);
	}
	elsif ($tagname eq "datastreamdef"){

		$datastreams{$dsname} = {
			   'dbconnstr' => $dbconnstr,
						'csv' => $csv,
						'sql' => $sql,
					'breaks' => [@breaks],
				  'targets' => [@targets]};

		#  host, port, options, tty, dbname, user, password
		printdbg( 2, "\tBreaks: @{$datastreams{$dsname}{'breaks'}}" );
		printdbg( 2, "\tTargetList: @{$datastreams{$dsname}{'targets'}}" );
		printdbg( 2, "\tcsv: $csv " );
		printdbg( 4, "\tSQL: $datastreams{$dsname}{'sql'}" );

		for (my $i=0; $i< @breaks ; $i++ ){

			if( $breaks[$i] eq 'NONE' ){next;}
			if( $breaks[$i] eq 'detail' ){next;}

			my $c = col2idx($dsname, $breaks[$i] );
			if ( $c == -1 ) {
				# oops.
				printdbg(1, "Break column '$breaks[$i]' not in TargetList for DataStream '$dsname'.");
				err(2, "Break column '$breaks[$i]' not in TargetList for DataStream '$dsname'.");
			}
		}
		#print Dumper( \%datastreams );
		$sql		= "";
		@breaks	= ();
		@targets	= ();
		printdbg( 1, "End DataStream Def: $dsname" );
	}
	elsif ($tagname eq "table" or $tagname eq "area") {

		if ($tagname eq "area" ) { $origtext = "<!--".$origtext."-->";}

		#
		# Find the last start area defined at this level.
		# It is our matching start area. 
		# We only care about named areas and tables.
		printdbg(3,"Finding matching tag: $level $origtext");
		my $backlevel;
		my $backidx;
		my $backorigtext;
		my @areamatch;
		for ( my $h = $htmlidx-1; $h >= 0 ; $h-- ){
			($backlevel, $backidx, $backorigtext, @areamatch ) = @{$HTML[$h]};

			if( ( ($backorigtext =~ m/<table/i and $tagname eq "table")
				or ( $backorigtext =~ m/<area/i and $tagname eq "area") )
				and $backlevel == $level){

				# our matching tag is not named, print it and go on
				if ($backorigtext !~ 'name=' ) {
					printdbg(3,"\t$level $origtext matches $backlevel $backidx $backorigtext");
					printit ( $origtext );
					$level = $level - 1;
					if ($level == 1){
						$self->handler(text => undef);
					}
					return;
				}
				else { 
					printdbg(3,"\t$level $origtext matches $backlevel $backidx $backorigtext");
					my $currarea	=  $areamatch[0][0];
					my @dss			=	@{$areamatch[0][1]};
					my $maxrepeat	=	$areamatch[0][2];
					my $currbreak	=	$areamatch[0][3];
					my $areastart	=	$areamatch[0][4];
					my $areaend		=	$htmlidx;
					my $areacheck	=	$areamatch[0][6];
					my @newarea		=	($currarea, [@dss], $maxrepeat, $currbreak, $areastart, $areaend, $areacheck);

					# pop this (these) datastreams from the openareads list
					#printdbg(2, "before pop openareads: @openareads ");
					for (my $a=0; $a < @dss ; $a++){ 
						pop @openareads; 
						#printdbg(2, "after pop openareads: @openareads ");
					}

					# Set the end area index and save it for both
					# the begin area and the end area HTML.
					push @HTML, [$level,$htmlidx, $origtext, [@newarea] ];
					$HTML[$backidx] = [$backlevel,$backidx, $backorigtext, [@newarea] ];
					printdbg(4, "Reset HTML[$backidx] with $currarea, @dss, $maxrepeat, $currbreak, $areastart, $areaend, $areacheck");
					printdbg(4, "pushHTML[$htmlidx] with $level,$htmlidx, $origtext, and same areainfo");

					$htmlidx = $htmlidx +1;
					$level = $level - 1;
					if ($level == 1){
						$self->handler(text => undef);
					}
					return; 
				}
			}
		}
		printdbg(1, "Could not find end of table or area: $level $htmlidx $origtext");
		err(2,"Could not find matching end of table or area level($level). Check your report .xml file for mismatched tables and areas.");
		return;
	}
	else { 
		printit( $origtext );
	}
	return;
}

sub printit
{
	# Eliminate extra tabs and newlines.
	# Add a space to the end of the line
	# JUST IN CASE the parse buffer ended
	# on a variable which needs a trailing
	# non-word to be able to be recognized.
	# Don't save blank lines.
	# See substitutions.
	my $origtext	= shift;
	#$origtext		=~ s/[\t\n\r]//g;

	if ($origtext =~ /\S$/){
		$origtext		=~ s/$/ /;
	}
	push @HTML, [ $level, $htmlidx, $origtext ];
	printdbg (4,"pushHTML: $level, $htmlidx, $origtext");
	$htmlidx = $htmlidx + 1;
}
# ============== END OF PARSER CALLBACKS =====================

# ============== MAIN: COMPILE =====================
if ( $ARGV[0] =~ /-v/){
	die "$version\n";
}
for (my $i=1; $i<@ARGV; $i++){
	if ( $ARGV[$i] =~ /=/){
		my ($name, $value) = split '=', $ARGV[$i];
		$value = arg_rules($name, $value); # values not starting w/(( are s-quoted
		$var{$name} = $value;
		print "<!-- ARGUMENT: $name=$value -->\n";
	}
}
if ( $var{'debug'} ){ $debug = $var{'debug'}; }
if ($debug > 0 ){
	my $file;
	my $rp = "";
	my $rn = "";
	if ( defined $var{'debugdir'} or defined $var{'rootdir'} ) {
		$rp = $var{'rootdir'}."/".$var{'debugdir'}."/";
		$report =~ /\/[\w_\.\/]+\/([\w_\. ]+)/ ;
		$rn = $1;
		if ( $rn eq "" ) { $rn = $report; } 
		$file = $rp.$rn.".dbg";
	}
	else {
		$file = "$report.dbg";
	}
	print "<!-- DEBUG FILE: $file -->\n";
	if ( !(open DEBUG, "> $file ") ) {
		print "<!-- DEBUG FILE: Cannot open debug file. Continuing anyway $! -->\n";
	  #die "Cannot open debug file '$file': $!"; 
	}
}
else {
	open DEBUG, "> /dev/null" or die "Cannot open /dev/null. Uh,oh: $!";
}

printdbg(1,"Debug Level=$debug");
print DEBUG Dumper(%var);
my $dsnow;
my $p = HTML::Parser->new(api_version => 3,
		default_h => [\&printit, "text"],
		  start_h => [\&start, "self, tagname, attr, text"],
			 end_h => [\&end, "self, tagname, text"],
			text_h => [\&printit, "text"] );

if ( !defined $var{'suffix'} ){
	$var{'suffix'} = '.xml'; 
	if (!(open JUSTTESTING, "< $report$var{suffix}") ) {
		if( open JUSTTESTING, "< $report.ere" ) {
	 		$var{'suffix'} = '.ere'; 
		}
		else {
			printdbg(1,"Cannot open report file $report.xml or $report.ere");
			err(2, "ERROR: Cannot open report file $report.xml or $report.ere");
		}
	}
	close JUSTTESTING;
}
printdbg(1,"Report=$report$var{'suffix'} Suffix=$var{'suffix'}");
$p->parse_file("$report$var{'suffix'} " || die "Bad report name _ $report _ ") ||
	die "Bad report file '$report$var{'suffix'}': $!";

# ============== END MAIN: COMPILE =====================

# ============== START RUN =====================
printdbg (1, "RUN=====================================================");
#print DEBUG Dumper(@HTML);
run();
# ============== END RUN =====================


# ============== BEGIN SUBROUTINES  =====================
sub run ()
{
	my $currarea		= "";		# current area name
	my $currds			= "";		# current datastream name
	my $currbreak		= "";		# break of current area
	my $maxrepeat		= -1;		# repeat count defined for area
	my $areacheck		= "no";	# fetch next row:see area object def
	my $areaend			= 0; 		# Index of the end of an area
	my $repeatcount	= 0;		# current repeats of current area
	my $htmlidx			= -1;		# Index into HTML array
	my $thisbreak 		= 0;		# Boolean--is this our break?
	my %arearepeatcount;			# hash of current repeats of area by area name
	my $val;							# substitution value
	my $ok;							# return val from fetchrow
	my @dss;							# list of current datastreams

	for (my $i = 0; $i < @HTML ; $i++)
	{
		my ($curlevel, $curhtmlidx, $line, @details) = @{$HTML[$i]};
		#
		# Start and End Areas are the only sections with details
		#
		printdbg (5,"$curlevel: $line");
		if ( !@details ){ 

			# substitute columns and variables in HTML
			while( $line =~ /_([a-z_\.]+)_[\W ]+/ ) {
				$column = $1;
				$val = getval( $column, @dss );
				$line =~ s/_$column\_/$val/;
					printdbg(2,"Substituting column: $column with $val");
			}
			$line = subvars ($line );
			#while( $line =~ /:([a-z_\.]+):[\W ]+/g ) {
				#$column = $1;
				#if ( $var{$column} ){
					#$line =~ s/:$column:/$var{$column}/;
					#printdbg(2,"Substituting variable: $column with $var{$column}");
				#}
				#else {
					#$line =~ s/:$column:/ /; 
					#printdbg(2,"Substituting variable: $column with nothing");
				#}
			#}

			printline($curlevel,$line); 

		}
		else { # initialize area

			$currarea	=  $details[0][0];
			@dss			=  @{$details[0][1]};
			$maxrepeat	=  $details[0][2];
			$currbreak	=  $details[0][3];
			$htmlidx		=  $details[0][4];
			$areaend		=  $details[0][5];
			$areacheck	=  $details[0][6];

			printdbg (5,
				"Area Details: HTML[$i]\n\t($currarea, (@dss), $maxrepeat, $currbreak, $htmlidx, $areaend, $areacheck)" );
			# print DEBUG Dumper( @dss);

			#
			# Start areas have a start html index which corresponds
			# to the one in their details.
			#
			if ( $i == $htmlidx ) { 

				#
				# Area Check:
				# Check to see if we should do or skip this area
				# IF the break is us, fetch the next row on entry to our area.  
				# --> This is used for sibling areas bounded by maxrepeats.  
				# At the top of sibling <next>, fetch the next row 
				# so we don't repeat the row at the bottom of sibling <prev>.
				# IF the break is not us, SKIP the area by jumping
				# to the end of the area.
				if ($areacheck eq "yes" ) {
					printdbg (2,"Areacheck: break: $currbreak");
					$thisbreak = thisbreak($dss[0], $currbreak);
					if ($thisbreak) {
						# fetch and start area
						# if more than one datastream, fetch them all.
						printdbg (2,"This is the break: $currbreak");
						for (my $d=0; $d < @dss; $d++){
							$currds = $dss[$d];
							$ok = fetchrow( $currds );
						}
					}
					else {
						# jump to end of area
						printdbg (1,"Not this break $currbreak. Jump to end of area");
						$i = $areaend;
					}
				}

				# This is a repetition, but not the first repetition
				if ($arearepeatcount{$currarea} and
						$arearepeatcount{$currarea} > 1 ){
					printline($curlevel, "<!--Start area $currarea $currbreak-->"); 
					printdbg(1,"<!--Start area $currarea $currbreak-->"); 
				}
				# First entry
				else {
					for (my $d=0; $d < @dss; $d++){
						$currds = $dss[$d];
						if ( !initds( $currds ) ) {
							# Should never get here. initds checks for valid data.
							printdbg( 1, "No valid data returned for query.");
							err(1, "ERROR: No valid data returned for query.");
						}
					}

					$arearepeatcount{$currarea}= 1;
					printline($curlevel, "<!--Start area $currarea $currbreak-->"); 
					printdbg(1,"<!--Start area $currarea $currbreak-->"); 
					printline($curlevel,$line); 
				}

			}
			# 
			# End Areas have details, and have the start htmlidx in the detail
			#
			else { # end area
				$currarea	=  $details[0][0];
				@dss			=  @{$details[0][1]};
				$maxrepeat	=  $details[0][2];
				$currbreak	=  $details[0][3];
				$htmlidx		=  $details[0][4];
				$areaend		=  $details[0][5];
				$areacheck	=  $details[0][6];
				printdbg (3,
					"END area $currarea $currbreak repeat: $arearepeatcount{$currarea} check next? $areacheck"); 

				#
				# Repeat handler
				# This is very key, delicate code. 
				# If there are two datastreams, both have the same currbreak
				# If we have a break matching ours, repeat the area
				# (but check the repeat counting stuff first)
				#
				$thisbreak = thisbreak($dss[0], $currbreak);
				printdbg(1,"Checking for break $currbreak ");
				printdbg(3,"\tthisbreak=$thisbreak Maxrepeat=$maxrepeat Repeatcount= $arearepeatcount{$currarea}");

				# The break is us and repeats are unlimited
				#  OR The break is us and repeats have room left
				# repeat if we have data
				if ( $thisbreak and ( ($maxrepeat == -1) or
					 ($maxrepeat > $arearepeatcount{$currarea} ) ) ){

					printdbg (1,"This is the break: $currbreak");

					# fetch and repeat area
					# if more than one datastream, fetch them all.
					for (my $d=0; $d < @dss; $d++){
						$currds = $dss[$d];
						$ok = fetchrow( $currds );
					}
					if ( $ok  ){
						$i = $htmlidx ; 
						$arearepeatcount{$currarea} = $arearepeatcount{$currarea} +1
					}
					# continue, no more data
					else {
						printdbg (1,"NO MORE DATA at $currbreak ");
						$arearepeatcount{$currarea} = 1;
						printline($curlevel,$line); 
					}
				}
				# Not our break OR we reached maxrepeat
				else {
					# continue, no breaks
					printdbg (1,"Not this break $currbreak. Fall through.");
					$arearepeatcount{$currarea} = 1;
					printline($curlevel,$line); 
				}
			}
		}
	}
}
# ============== BEGIN DATASTREAM SUBROUTINES  =====================
sub printline
{
	my $level	= $_[0];
	my $text		= $_[1];
	my $old		= select STDOUT; # make sure we are not outputting to debug

	for (my $j=0; $j < $level; $j++) { print "\t";}
	print "$text\n"; 
	select $old;
}

sub checkrowdefs
{
	my $result	= $_[0];
	my @row		= $result->fetchrow;

	if ( defined @row ) {
		for( my $i=0; $i < @row; $i++){
			if (!defined $row[$i]) {
				$row[$i] = ' ';
			}
		}
	}
	return @row;
}

sub fetchrow
{
	my $b;
	my $dsname = $_[0];

	if ( !$datastreams{$dsname} ) { 
		# should not get here
		printdbg (1,"Fetch Row BAD DataStream NAME: $dsname:"); #DEBUG
		err(1, "INTERNAL ERROR: fetch row: bad datastream name $dsname \n");
	}
	my $ds		= $datastreams{$dsname};
	my $result	= $ds->{'result'};
	my @currrow	= $ds->{'currrow'} ;

	# if first (no row)
	#	fetch curr, next
	#	set currb as all
	#	nextb = diff currrow/nextrow
	# else
	#	move nextrow currrow
	#	fetch next row
	#	move nextb to currbrk
	#	set next brk = diff currrow, nextrow
	if ( !$ds->{'currrow'} ){
		printdbg (1, "Fetch First Row:"); #DEBUG

		$ds->{'currrow'} = [checkrowdefs($result)];
		if ( $ds->{'csv'} eq "yes" and defined $var{'do_csv'} and $var{'do_csv'} eq "t" ) { 
			# my $csv = $ds->{'csv'};
			printdbg (1, "Creating csv output for $dsname "); #DEBUG
			printcsv($result, $dsname );
		}
		if( $ds->{'currrow'} ) {
			$ds->{'nextrow'} = [checkrowdefs($result)]; 
		}

		#
		# DEBUG
		#
		printdbg (3,"\tCurrrow: @{$ds->{'currrow'}}"); #DEBUG
		if( $ds->{'nextrow'} and @{$ds->{'nextrow'}} ) { #DEBUG
			printdbg (3,"\tNextrow: @{$ds->{'nextrow'}}"); #DEBUG
		} #DEBUG

		# initialize break structure to 0s and set rightmost break to 1.
		for ($b=0; $b < @{$ds->{'breaks'}}; $b++ ){
			$ds->{'nextbrk'}[$b] = 0; 
		}
		# In the case of no breaks (singleton select) put in a detail break
		if ( $b != 0 ) {$ds->{'nextbrk'}[$b-1] = 1;}
		else  {$ds->{'nextbrk'}[$b] = 1;}

		setnxtbreak( $dsname );
		return 1; # got data
	}
	else {
		printdbg (1, "Fetch Next Row:"); #DEBUG
		if( defined $ds->{'nextrow'} and @{$ds->{'nextrow'}} ){
			$ds->{'currrow'} = [@{$ds->{'nextrow'}}];
			if( defined $ds->{'currrow'} ) { 
				printdbg (3,"\tCurrrow: @{$ds->{'currrow'}}"); #DEBUG
			}
			else {
				printdbg(2,"CurrentRow not defined");
			}

			$ds->{'nextrow'} = [checkrowdefs($result)]; 
			if ( defined $ds->{'nextrow'} and @{$ds->{'nextrow'}} ) {
				printdbg (3,"\tNextRow: @{$ds->{'nextrow'}}"); #DEBUG
			}
			else {
				printdbg(2,"NextRow not defined or is empty");
			}
			setnxtbreak( $dsname );
			return 1; # got data
		}
		else {
			printdbg (3,"\tEnd of data"); #DEBUG
			return 0; # no more data
		}
	}
	return 0; # no more data
}
#
# Check DataStream Breaks
#
sub thisbreak
{
	my $dsname		= $_[0];
	my $breakname	= $_[1];
	my $debugstring;

	if ( !defined $datastreams{$dsname} ) { 
		# should not get here
		printdbg(1,"oops, thisbreak: bad datastream name $dsname \n");
		err(1,"INTERNAL ERROR: thisbreak: bad datastream name $dsname \n");
	}
	my $ds = $datastreams{$dsname};

	# We are looking for the leftmost break.  
	# Breaks are 0 (false) or 1 (true) in the order of the breaklist
	# If it is us, we are this break
	# otherwise, nevermind
	for ( my $b = 0; $b < @{$ds->{'breaks'}}; $b++ ){

		printdbg (4," breaks[$b] = $ds->{'breaks'}[$b] "); #DEBUG`
		printdbg (4,"nextbrk[$b] = $ds->{'nextbrk'}[$b] "); #DEBUG

		# Found the leftmost break (else loop back to top)
		if ( $ds->{'nextbrk'}[$b] == 1 ){
			# Yes, it is us
			if ( $ds->{'breaks'}[$b] eq $breakname ) {
				printdbg (2,"This Break $dsname: $breakname"); #DEBUG
				return 1;
			}
			else {
				#
				# not this break, but another break
				#
				printdbg (2,"Looking for break $breakname found $ds->{'breaks'}[$b]"); #DEBUG
				printdbg_breaks(); # DEBUG
				return 0;
			}
		}
	}
	printdbg (2,"No Breaks Found $dsname, $breakname"); #DEBUG
	printdbg_breaks(); # DEBUG
	return 0;
}
#
# Set DataStream Breaks
#
sub setnxtbreak
{
	my $dsname = $_[0];

	if ( !$datastreams{$dsname} ) { 
		# should not get here
		printdbg(1,"oops, setnxtbreak: bad datastream name $dsname");
		err(1,"INTERNAL ERROR: setnxtbreak: bad datastream name $dsname \n");
	}
	my $ds 				= $datastreams{$dsname};
	my $debugstring	= "";
	my @nextrow			= ();
	my @currrow			= ();

	# Set the current breaks values to the next row's break values
	$ds->{'currbrk'}	= [@{$ds->{'nextbrk'}}];

	if ( defined $ds->{'currrow'} and @{$ds->{'currrow'}} ) {
		@currrow = @{$ds->{'currrow'}};
	}
	else {
		printdbg(1,"Setting Breaks for $dsname: Current Row not defined or empty");
		return;
	}

	if ( defined $ds->{'nextrow'} and @{$ds->{'nextrow'}} ) {
		@nextrow = $ds->{'nextrow'};
	}
	else {
		# Clear out the next break values
		for ( my $b = 0; $b <@{$ds->{'breaks'}}; $b++ ){
			$ds->{'nextbrk'}[$b] = 0;
		}
		printdbg(1,"Setting Break for $dsname: Nextrow not defined or empty");
		return;
	}


	#
	# debug printing
	#
	if( defined @currrow and @currrow ){
		printdbg (5,"Setting Breaks for $dsname: Current Row: [ @{$ds->{'currrow'}} ]");
	}
	else {
		printdbg (1,"Setting Breaks for $dsname: Current Row is empty!");
	} # DEBUG
	if( defined @nextrow and @nextrow ) {
		printdbg (5,"Setting Breaks for $dsname: [ @{$ds->{'nextrow'}} ] ");
	}
	else {
		printdbg (1,"Setting Breaks for $dsname: Next Row is empty!");
	} # DEBUG

	for ( my $b = 0; $b <@{$ds->{'breaks'}}; $b++ ){

		# no more data, no more breaks
		# detail breaks are always true if there is data
		if ( $ds->{'breaks'}[$b] eq 'detail' ){
			printdbg (3,"Setting Break for $dsname: detail break, always true");# DEBUG
			$ds->{'nextbrk'}[$b] = 1;
		}
		# areas with no breaks have a "NONE" break;
		elsif ( $ds->{'breaks'}[$b] ne 'NONE' ){
			my $i = col2idx($dsname, $ds->{'breaks'}[$b] );
			if ( $i == -1 ) {
				# Should not get here.
				printdbg(1, "Column $ds->{'breaks'}[$b] not in target list for $dsname.");
				err(1, "INTERNAL ERROR: Column $ds->{'breaks'}[$b] not in target list for $dsname.\n");
			}
			if ($ds->{'nextrow'}[$i] && 
				($ds->{'currrow'}[$i] eq $ds->{'nextrow'}[$i])){
				$ds->{'nextbrk'}[$b] = 0;
			}
			else {
				$ds->{'nextbrk'}[$b] = 1;
				printdbg (2,"Setting Break for $dsname: $ds->{'breaks'}[$b]");# DEBUG
			}
		}
		else {
			printdbg (3,"Set detail Break for $dsname");# DEBUG
			$ds->{'nextbrk'}[$b] = 0;
		}
	}

	#
	# more debug printing
	#
	printdbg_breaks(); # DEBUG
}

#
# Initialize the datastream 
#
sub initds {
	my $dsname	= $_[0];
	my $ds		= $datastreams{$dsname};
	my $result_status;
	my $ok;

	if ( !defined $ds ) { 
		# should not get here
		printdbg(1, "oops, initds: bad datastream name $dsname");
		err(1, "INTERNAL ERROR: initds: bad datastream name $dsname \n");
	}
	if ( $ds->{'currrow'} ) {
		return 1; # already initialized
	}

	printdbg (1,"InitDS $dsname"); #DEBUG
	my $query = $ds->{'sql'};
	my $dbconnstr = $ds->{'dbconnstr'};
	if ( $dbconnstr eq "" ) {
		my $Option_ref	= Pg::conndefaults();
		$dbconnstr = "dbname=$$Option_ref{dbname}";
	}

	my $result;

	# Get default connection values. Connect or die.
	# Probably should be nicer.
	$conn	= Pg::connectdb( $dbconnstr );

	if (PGRES_CONNECTION_OK ne $conn->status ){
		my $msg = $conn->errorMessage ;
		print "Connection Failure: $msg \n";
		printdbg(1, "Connection Failure: $msg  ");
		err(1, "Connection Failure: $msg \n");
	}
	$ds->{'conn'} = $conn;

	printdbg (1,"Exec($query)\n");
	$result = $conn->exec( $query);
	$result_status = $result->resultStatus;
	if ($result_status != PGRES_TUPLES_OK ){
	my $msg = $conn->errorMessage ;
		printdbg(1, "INTERNAL SQL ");
		printdbg(1, "\t $msg ");
		err(3, "SQL $msg ");
	}
	if ($result->ntuples <= 0){
		printdbg(1, "No valid data selected.");
		err(1, "No valid data was selected.\n");
	}

	$ds->{'result'} = $result;
	$ok = fetchrow( $dsname );
	push @rtds, $dsname;
		printdbg(4, "RTDS: @rtds ");
	return $ok;
}	

sub getval
{
	my $col = shift;
	#my @dss = @_;	# DataStream Name
	my @dss = @rtds;	# DataStream Name
	my $val;			# return value
	for (my $d=0; $d < @dss; $d++){
		$dsname = $dss[$d];
	
		my $ds = $datastreams{$dsname};
		printdbg(5,"Getval: $d Checking $dsname for $col "); 

		my $idx = col2idx($dsname, $col);	# Column Name
		if ( $idx == -1 ) {
			next;
		}
		
		if( defined $ds->{'currrow'} and defined $ds->{'currrow'}[$idx]) {
			$val = "$ds->{'currrow'}[$idx]";
		}
		else {
			$val = " " ;
		}

		if( $val ne "" ){printdbg (3, "Getval($col): $val" );}
		else { printdbg (3, "Getval($col): empty val" );}

		return "$val";  
	}
	# oops.
	printdbg(1, "ERROR: gv: Column $col not in target list for @dss.");
	return "***$col***";
}
# ============== END DATASTREAM SUBROUTINES  =====================
# ============== BEGIN HELPER SUBROUTINES  =====================

# get the target list index for this column 
# from the current datastream. 
sub col2idx
{
	my $curds	= $_[0];
	my $col		= $_[1];
	my $ds		= $datastreams{$curds};

	for( my $i=0; $i < @{$ds->{'targets'}}; $i++ )
	{
		if ( $col eq $ds->{'targets'}[$i] ) { 
				return $i; 
		};
	}
	return -1;
}

sub err
{
	my $howbad	= $_[0];
	my $msg		= $_[1];
	my $header	="<P><table border=1>\n<tr><td colspan=2>Report Parameters</td></tr>\n<tr><th>Name</th><th>Value</th></tr>";
	my $footer	="</td></tr></table>\n";
	my $varmsgs	= "";

	if( $howbad == 0 ){ # not so bad
		printdbg (0,"$msg\n");
		return;
	}
	# Make an effort to close out open tables.
	for (my $i=1; $i < $level; $i++){
		print "</table>\n";
	}
	print "<H3>Report '$reportname' failed to run.</H3>\n";
	print "<table border=1>\n";
	if ( $howbad == 1 ) { # runtime die bad
		print "<tr><th>Report Run Time Error</th></tr>\n";
		print "<tr><td> $msg </td></tr>\n";
		print "</table>\n";
	}
	elsif ($howbad == 2 ) { # compile time die bad
		print "<tr><th>Report Compile Time Error</th></tr>\n";
		print "<tr><td> $msg </td></tr>\n";
		print "</table>\n";
	}
	elsif ($howbad == 3 ) { # runtime sql die
		print "<tr><th>Report Query Error</th></tr>\n";
		print "<tr><td> $msg </td></tr>\n";
		print "</table><P>\n";
	}
	for (my $i=1; $i<@ARGV; $i++){
		if ( $ARGV[$i] =~ /=/){
			my ($name, $value) = split '=', $ARGV[$i];
			if ( $name !~ /csv/ and $name !~ /debug/ and $name !~ /rootdir/ ){
				$varmsgs= $varmsgs."<tr><td>$name</td><td>$value</td></tr>\n";
			}
		}
	}
	print "<P><B>Ensure your report parameters are correct.</B><BR>";
	print "<UL>\n";
	print "<LI> A message indicating a <I>bad representation, read or conversion</I>";
	print " suggests that one or more of the parameters was not in";
	print " an appropriate format.\n";
	print "<LI> If <I>no valid data was selected,</I> check your date and time parameters";
	print " and other parameters for valid values.";
	print " It is possible that there may be no valid data for that time period";
	print " for an otherwise valid argument.";
	print "<LI> If the error message is something other than these, please ";
	print "report the error to your report developers.";
	print "</UL>\n<P>";
	if ($varmsgs ne "" ){
		$varmsgs = $header.$varmsgs.$footer;
		print $varmsgs;
	}
	print "<P><B>Reporting an error</B> \n <LI> Use your browser to email this page to ";
	print "your report developers";
	print "<LI>Contact them ";
	print "with the report name, the error message and a description of the problem.";
	die $msg;
}

sub printdbg
{
	my $old;
	my $msglevel = $_[0];
	my $msg = $_[1];
	if ( $msglevel <= $debug ){
		$old = select DEBUG ; $| = 1; 
		print DEBUG "$msglevel: $msg\n";
		select $old ;
	}
}
sub printcsv
{
	my $result	= $_[0];
	my $dsname	= $_[1];
	my $file		= $var{$dsname};

	# filename has csvdir prepended to it already.
	printdbg (1, "CSV file for $dsname : $file"); #DEBUG
	if ( defined $var{rootdir} ) {
		$file = sprintf( "%s/%s", $var{'rootdir'},$file );
	}

	if ( !(open CSV, "> $file ") ) {
		err(1,"Cannot open CSV file: $file $!"); 
	}

	my $fname;
	my $old		= select CSV; $| = 1;
	my $nfields	= $result->nfields();
	for ( my $f=0; $f < $nfields; $f++){
		$fname = $result->fname($f);
		print CSV "$fname";
		if ($f < $nfields -1 ) {print CSV ",";}
	}
	print CSV "\n";
	# $result->print($fout, $header, $align, $standard, $html3, $expanded, $pager, $fieldSep, $tableOpt, $caption, ...)
	$result->print( \*CSV,0,0,0,0,0,0,',','','');
	close CSV;
	select $old;
}
#
# DEBUG printing of breaks
#
sub printdbg_breaks
{
	my $ds = $datastreams{$dsname}; # this better be valid!
	if ( !defined $ds ) {
		printdbg (4,"Breakprint: no DataStream"); # DEBUG
		return;
	}
	if ( !defined $ds->{'breaks'} ) {
		printdbg (4,"Breakprint: no Breaks"); # DEBUG
		return;
	}
	if ( !defined $ds->{'currbrk'} ) {
		printdbg (4,"Breakprint: no Current Row Breaks:"); # DEBUG
		return;
	}
	if ( !defined $ds->{'nextbrk'} ) {
		printdbg (4,"Breakprint: no Next Row breaks"); # DEBUG
		return;
	}


	#
	# Break Names
	my $debugstring = " ";
	for ( my $b = 0; $b < @{$ds->{'breaks'}}; $b++ ){
		$debugstring = $debugstring." $ds->{'breaks'}[$b] ";
	}
	printdbg (2,"\tBreak names: $debugstring"); # DEBUG
	# Curr Break Values
	$debugstring = " ";
	for ( my $b = 0; $b < @{$ds->{'breaks'}}; $b++ ){
		$debugstring = $debugstring." $ds->{'currbrk'}[$b] ";
	}
	printdbg (2,"\tCurrent Break Values:$debugstring"); # DEBUG
	# Next Break Values
	$debugstring = " ";
	for ( my $b = 0; $b <@{$ds->{'breaks'}}; $b++ ){
		$debugstring = $debugstring." $ds->{'nextbrk'}[$b] ";
	}
	printdbg (2,"\tNext Break Values:$debugstring"); # DEBUG
}

sub arg_rules
{
	my $name = $_[0];
	my $value = $_[1];

	if ( (($name =~ m/time/ and !($value  =~ m/curr/) ) or 
	     ($name =~ m/date/ and !($value  =~ m/curr/) ))
		and $value ne "" ) {
		return "'$value'";
	}
	else {
		return $value;
	}

}

sub subvars
{
	my $line = $_[0];
	while( $line =~ /:([a-z_\.]+):[\.\W ]+/g ) {
		$column = $1;
		if ( $var{$column} ){
			$line =~ s/:$column:/$var{$column}/;
			printdbg(2,"Substituting variable: $column with $var{$column}");
		}
		else {
			$line =~ s/:$column:/ /;
			printdbg(2,"Substituting variable: $column with nothing");
		}
	}
	return $line;

}

# ============== END HELPER SUBROUTINES  =====================
