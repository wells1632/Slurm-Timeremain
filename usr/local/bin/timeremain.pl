#!/usr/bin/perl

# Script to show how much time is remaining (max) for a given node
# Version 1.0

# Handy commands...
# squeue -O timeleft,nodelist -w c21a-s1
# scontrol show hostname 

use Text::ParseWords;
use DBI;
use strict;
use POSIX qw(strftime);
use Getopt::Std;

# Flag variables
my $debug=0;
my $order="ORDER BY TIMEREMAIN DESC, NODE ASC";

# Location of DSH group files
my $DSH="/etc/dsh/group/"

# Get Commandline Options
getopts('dhmnrwR:p:D:');
our($opt_h, $opt_d, $opt_n, $opt_r, $opt_m, $opt_w, $opt_R, $opt_p, $opt_D);
if($opt_h) {
    print "Displays time remaining for jobs on nodes\n";
    print "\n";
    print "Usage: $0 [options] [nodelist]\n";
    print "Options: \n";
    print "-d: Debug mode\n";
    print "-h: This help\n";
    print "-m: Omit nodes that are reserved or in maintenance\n";
    print "-n: Order results by node (by time is default)\n";
    print "-r: Reverse order of results\n";
    print "-w: Print job ID's associated with nodes\n";
    print "-R: Use reservation for nodelist\n";
    print "-p: Use partition for nodelist\n";
    print "-D: Use DSH group\n";
    print "\n";
    print "Nodelist: Nodes can be comma separated or described in SLURM notation, i.e. c1a-s[3-7]\n";
    exit 0;
}
if($opt_d) {
    $debug=1;
}
if ($opt_r) {
    $order="ORDER BY TIMEREMAIN ASC, NODE ASC";
}
if ($opt_n) {
    if ($opt_r) {
	$order="ORDER BY NODE DESC, TIMEREMAIN DESC";
    } else {
	$order="ORDER BY NODE ASC, TIMEREMAIN DESC";
    }
}

# Check for specific nodes instead
my $specNodes;
if($ARGV[0]) {
    $specNodes = $ARGV[0];
}

# A partition list is requested
if($opt_p) {
    $specNodes = `sinfo -p $opt_p | tail -n +2 | awk '{print \$6}' | tr "\\n" ","`;
}

# A reservation list is requested
if($opt_R) {
    $specNodes = `sinfo -T | grep $opt_R | awk '{print \$6}' | tr "\\n" ","`;
}

# A DSH group is requested
if($opt_D) {
    # Check to ensure that DSH group exists
    if (-e "${DSH}${opt_D}") {
	$specNodes = `cat ${DSH}${opt_D} | tr '\\n' ','`;
	chop($specNodes);
    } else {
	die "The DSH group $opt_D does not exist.";
    }
}


# Setup temporary database
my $driver = "SQLite";
my $dsn = "DBI:$driver:dbname=:memory:";
my $userid = "";
my $password = "";
my $dbh = DBI->connect($dsn, $userid, $password, { RaiseError => 1 })
    or die $DBI::errstr;
if ($debug){
    print "Opened database successfully\n";
}

# Create table
my $stmt = qq(CREATE TABLE TIMEREMAIN
  (
    NODE          TEXT PRIMARY KEY,
    TIMEREMAIN    INT  NOT NULL););
my $rv = $dbh->do($stmt);
if ($rv <0) {
    print $DBI::errstr;
} else {
    if ($debug) {
	print "Table created successfully\n";
    }
}

# First, we create entries in the table with zero times. This is so that we have a list of
# nodes that are not being used as well
my @sinfo = `sinfo -Nehl | awk \'\{print \$1\}\' | sort | uniq`;
foreach $a (@sinfo) {
    my @nodes;
    chomp($a);
    $stmt = qq(INSERT INTO TIMEREMAIN (NODE, TIMEREMAIN)
	       VALUES ('$a', '0'));
    $rv = $dbh->do($stmt) or die $DBI::errstr;
}

# Grab squeue information
my @squeue = `squeue -o "%L %N" | tail -n +2`;

# Parse through squeue data
foreach $a (@squeue) {
    my @line = &shellwords($a);
    # Check for single value line - if it is, we don't use it
    if($#line != 0) {
	if (@line[0] != 'INVALID') {
	    # At this point we should have good information... we just need to make it clean
	    # Parse time information for the line
	    my $tottime = 0;
	    if(index(@line[0], '-') != -1) {
		# This is a time value that is longer than a day
		my @timeval = split(/-/,@line[0]);
		$tottime += @timeval[0] * 86400;
		my ($h,$m,$s)=split(/:/,@timeval[1]);
		$tottime += $h*3600;
		$tottime += $m*60;
		$tottime += $s;
		if($debug) {
		    print "Found time that is longer than a day on @line[1], Time = " . convert_seconds_to_hhmmss($tottime) . "\n";
		}
	    } else {
                # Time reported by squeue chops off leading zeros in times, so if the time is under an hour it will only display
                # mm:ss, and if under a minute it will only display seconds. Have to try each case.
                my $char_count = () = @line[0] =~ /:/g;
                my ($h,$m,$s)=split(/:/,@line[0]);
                if($char_count == 2) {
                    $tottime += $h*3600;
                    $tottime += $m*60;
                    $tottime += $s;
                }
                elsif($char_count >= 1) {
                    $tottime += $h*60;
                    $tottime += $m;
                }
                else {
                    $tottime +=$h;
                }
	    }
	    if($debug) {
		print "Current time on @line[1] is " . convert_seconds_to_hhmmss($tottime) . "\n";
	    }
	    # Now split out the values of the nodes associated with this time and insert into the database
	    my @nodes;
	    if(index(@line[1], '[') != -1) {
		@nodes = `scontrol show hostname @line[1]`;
	    } elsif ( index(@line[1], ',') != -1) {
		@nodes = split(/,/, @line[1]);
	    } else {
		@nodes = (@line[1]);
	    }
	    foreach $b (@nodes) {
		chomp($b);
#		$stmt = qq(INSERT INTO TIMEREMAIN (NODE, TIMEREMAIN)
#			      VALUES ('$b', '$tottime'));
		$stmt = qq(SELECT TIMEREMAIN
                                  FROM TIMEREMAIN
                                  WHERE NODE = '$b');
		my $sth = $dbh->prepare($stmt);
		my $rv = $sth->execute() or die $DBI::errstr;
		my @checkVal = $sth->fetchrow_array();
		if ($debug) {
		    print "Current db TIMEREMAIN value: @checkVal[0] | Node: $b\n";
		}
		if (@checkVal[0]<$tottime) {
		    $stmt = qq(UPDATE TIMEREMAIN 
			       SET TIMEREMAIN = '$tottime'
			       WHERE NODE = '$b');
		    $rv = $dbh->do($stmt) or die $DBI::errstr;
		}
	    }
	}
    }
}

# A bit of debugging info
if($debug) {
    $stmt = qq(SELECT COUNT(NODE) FROM TIMEREMAIN);
    my $sth = $dbh->prepare($stmt);
    $rv = $sth->execute() or die $DBI::errstr;
    if($rv<0) {
	print $DBI::errstr;
    }
    while (my @row = $sth->fetchrow_array()) {
	print "Count = " . $row[0] . "\n";
    }
}

# Remove entries that are under maintenance or reserved if requested
if($opt_m) {
    @sinfo = `sinfo --state=RESERVED,MAINT| tail -n +2 | awk \'\{print \$6\}\'`;
    my $counter;
    foreach $a (@sinfo) {
	my @nodes;
	# Check if this is a slurm grouping
	if(index($a, '[') != -1) {
	    @nodes=`scontrol show hostname $a`;
	} elsif (index($a, ',') != -1) {
	    @nodes=split(/,/, $a);
	} else {
	    @nodes=($a);
	}
	foreach $b (@nodes) {
	    chomp($b);
	    $stmt = qq(DELETE FROM TIMEREMAIN
		       WHERE NODE = '$b');
	    $rv = $dbh->do($stmt) or die $DBI::errstr;
	    $counter++;
	}
    }
    if($debug) {
	print "Nodes removed from REMOVED and MAINT state: $counter\n";
    }
}
    

# Give a result
# Are we looking at a specific node?
if($debug) {
    print "SpecNodes value: $specNodes\n";
}
if($specNodes) {
    my @nodes;
    if(index($specNodes, '[') != -1) {
	@nodes = `scontrol show hostname $specNodes`;
	if($debug) {
	    print "Brackets\n";
	}
    } elsif (index($specNodes, ',') != -1) {
	@nodes = split(/,/, $specNodes);
	if($debug) {
	    print "Commas: '@nodes'\n";
	}
    } else {
	@nodes = ($specNodes);
	if($debug) {
	    print "Clean\n";
	}
    }
    my $where = "WHERE ";
    foreach $a (@nodes) {
	chomp($a);
	$where = $where . "NODE = '" . $a . "' OR ";
    }
    chop($where);
    chop($where);
    chop($where);
    $stmt = qq(SELECT DISTINCT NODE, MAX(TIMEREMAIN) FROM TIMEREMAIN 
	       $where
	       GROUP BY NODE
	       $order);
} else {
    $stmt = qq(SELECT DISTINCT NODE, MAX(TIMEREMAIN) FROM TIMEREMAIN 
	       GROUP BY NODE
	       $order);
}
if($debug) {
    print "SQL Statement: $stmt\n";
}
my $sth = $dbh->prepare($stmt);
$rv = $sth->execute() or die $DBI::errstr;
if($rv<0) {
    print $DBI::errstr;
}

if($opt_w) {
    printf "%15s %-15s %-15s\n", "Node", "Time Remaining", "Job ID's";
} else {
    printf "%15s %-15s %-15s\n", "Node", "Time Remaining";
}    
while (my @row = $sth->fetchrow_array()) {
    # Skip this line if the node name is two characters or less
    if (length(@row[0]) <= 2 ) {
	next;
    }
    my $convtime = convert_seconds_to_hhmmss(@row[1]);
    my $jobID;
    if($opt_w) {
	$jobID=`squeue -w @row[0] | tail -n +2 | awk '{print \$1}' | tr "\\n" ","`;
	chop $jobID;
    }
    if($debug) {
	print "Job ID's Generated: " . $jobID . "\n";
	printf "%15s - %s : %s - %s :: %s\n", @row[0], length(@row[0]), $convtime, @row[1], $jobID;
    } else {
	if($opt_w) {
	    printf "%15s %-15s %s\n", @row[0], $convtime, $jobID;
	} else {
	    printf "%15s %-15s\n", @row[0], $convtime;
	}
    }	
}

# Close out database and remove it
$dbh->disconnect();

# Subroutines
sub convert_seconds_to_hhmmss {
    my $hourz = 0;
    my $leftover=0;
    if($_[0] > 3600) {
	$hourz=int($_[0]/3600);
	$leftover=$_[0] % 3600;
    } else {
	$leftover=$_[0];
    }
    my $minz=int($leftover/60);
    my $secz=int($leftover % 60);
    if($debug) {
	print "DEBUG: total seconds: $_[0] | hourz : $hourz | minz : $minz | secz : $secz\n";
    }
    return sprintf ("%d:%02d:%02d", $hourz,$minz,$secz)
}
