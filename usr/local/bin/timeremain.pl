#!/usr/bin/perl

# Script to show how much time is remaining (max) for a given node
# Version 1.1

# Handy commands...
# squeue -O timeleft,nodelist -w c21a-s1
# scontrol show hostname 

use Text::ParseWords;
use DBI;
use strict;
use POSIX qw(strftime);

my $debug=0;

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
    NODE          TEXT NOT NULL,
    TIMEREMAIN    TEXT NOT NULL););
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
my @sinfo = `sinfo | tail -n +2 | awk \'\{print \$6\}\'`;
foreach $a (@sinfo) {
    my @nodes;
    # Check if this is a slurm grouping
    if(index($a, '[') != -1) {
	@nodes=`scontrol show hostname $a`;
    } elsif (index($a, ',') != -1) {
	@nodes=split($a, ',');
    } else {
	@nodes=($a);
    }
    foreach $b (@nodes) {
	chomp($b);
	$stmt = qq(INSERT INTO TIMEREMAIN (NODE, TIMEREMAIN)
		   VALUES ('$b', '0'));
	$rv = $dbh->do($stmt) or die $DBI::errstr;
    }
    
}

# Grab squeue information
my @squeue = `squeue -o "%L %128N" | tail -n +2`;

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
	    } else {
		my ($h,$m,$s)=split(/:/,@line[0]);
		$tottime += $h*3600;
		$tottime += $m*60;
		$tottime += $s;
	    }
	    # Now split out the values of the nodes associated with this time and insert into the database
	    my @nodes;
	    if(index(@line[1], '[') != -1) {
		@nodes = `scontrol show hostname @line[1]`;
	    } elsif ( index(@line[1], ',') != -1) {
		@nodes = split(@line[1], ',');
	    } else {
		@nodes = (@line[1]);
	    }
	    foreach $b (@nodes) {
		chomp($b);
		$stmt = qq(INSERT INTO TIMEREMAIN (NODE, TIMEREMAIN)
			      VALUES ('$b', '$tottime'));
		$rv = $dbh->do($stmt) or die $DBI::errstr;
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

# Give a result
$stmt = qq(SELECT DISTINCT NODE, TIMEREMAIN FROM TIMEREMAIN 
    ORDER BY NODE ASC, TIMEREMAIN DESC);
my $sth = $dbh->prepare($stmt);
$rv = $sth->execute() or die $DBI::errstr;
if($rv<0) {
    print $DBI::errstr;
}

while (my @row = $sth->fetchrow_array()) {
    my $convtime = convert_seconds_to_hhmmss(@row[1]);
    printf "%15s %s\n", @row[0], $convtime;
}

# Close out database and remove it
$dbh->disconnect();

# Subroutines
sub convert_seconds_to_hhmmss {
  my $hourz=int($_[0]/3600);
  my $leftover=$_[0] % 3600;
  my $minz=int($leftover/60);
  my $secz=int($leftover % 60);
  return sprintf ("%d:%02d:%02d", $hourz,$minz,$secz)
}
