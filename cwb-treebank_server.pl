#!/usr/bin/perl

use warnings;
use strict;

use IO::Socket;
use POSIX qw(:sys_wait_h SIGTERM SIGKILL);
use Fcntl qw(:flock);
use threads;
use threads::shared;
use JSON;
use DBI;
use File::Spec;
use Time::HiRes;

use CWB::CQP;
use CWB::CL;

use FindBin;
use lib $FindBin::Bin;
use CWB::treebank;

# read config
my %config = do "cwb-treebank_server.cfg";

POSIX::setuid( $config{"uid"} );

# fork once, and let the parent exit
{
    my $pidfile = $config{"pidfile"};
    my $pid     = fork;
    if ($pid) {
        open( my $PIDFILE, ">", $pidfile ) or die("Can't open $pidfile: $!");
        print $PIDFILE $pid;
        close($PIDFILE) or die("Can't close $pidfile: $!");
        exit;
    }
    die("Couldn't fork: $!") unless ( defined($pid) );
}

# redirect STDERR, STDIN, STDOUT
#open( STDERR, ">>", $config{"logfile"} )    or die("Can't reopen STDERR: $!");
#open( STDIN,  "<",  File::Spec->devnull() ) or die("Can't reopen STDIN: $!");
#open( STDOUT, ">",  File::Spec->devnull() ) or die("Can't reopen STDOUT: $!");

# dissociate from the controlling terminal that started us and stop
# being part of whatever process group we had been a member of
POSIX::setsid() or die("Can't start a new session: $!");

# clear file creation mask
umask 0;

# open logfile
open( my $log, ">>", $config{"logfile"} ) or die("Cannot open logfile: $!");

# make filehandle hot
{
    my $ofh = select $log;
    $| = 1;
    select $ofh;
}

my $server_port = $config{"server_port"};
my $server      = IO::Socket::INET->new(
    LocalPort => $server_port,
    Type      => SOCK_STREAM,
    ReuseAddr => 1,
    Listen    => 10
) or die("Couldn't be a tcp server on port $server_port : $@");

my $parent = $$;
my %child_processes : shared;

# don't fear the reaper ;o)
# avoid zombies
sub REAPER {
    while ( ( my $child_pid = waitpid( -1, WNOHANG ) ) > 0 ) {
        lock(%child_processes);
        delete $child_processes{$child_pid};
    }
    $SIG{CHLD} = \&REAPER;
}
$SIG{CHLD} = \&REAPER;

my $time_to_die = 0;
$SIG{INT} = $SIG{TERM} = $SIG{HUP} = sub { &log("Caught signal"); $time_to_die = 1; };

&log("Hello, here's your server speaking. My pid is $$");
&log("Waiting for clients on port #$server_port.");
while ( not $time_to_die ) {
    while ( ( my $client = $server->accept ) ) {
        if ( not $config{"clients"}->{ $client->peerhost() } ) {
            &log( sprintf( "Ignored conncection from %s", $client->peerhost() ) );
            next;
        }
        &log( sprintf( "Accepted conncection from %s", $client->peerhost() ) );
        my $pid = fork();
        die "fork: $!" unless defined $pid;
        if ( $pid == 0 ) {

            # we're the child
            &handle_connection($client);
            exit;
        }
        else {

            # we're the parent
            $client->close();
            &log("Forked child process $pid");
            {
                lock(%child_processes);
                $child_processes{$pid}++;
            }
            my $thread = threads->create( \&kill_child, $pid )->detach;
        }
    }
}

&log("Time to die");

sub handle_connection {
    my $socket = shift;
    my $output = shift || $socket;
    my $json = new JSON;
    my ( $cqp, %corpus_handles, $dbh );
    $SIG{INT} = $SIG{TERM} = $SIG{HUP} = sub { &log("Caught signal"); $socket->close(); undef($cqp); undef( $corpus_handles{$_} ) foreach ( keys %corpus_handles ); undef($dbh); exit; };
    $cqp = new CWB::CQP;
    $cqp->set_error_handler('die');
    $cqp->exec( "set Registry '" . $config{"registry"} . "'" );
    $CWB::CL::Registry = $config{"registry"};
    $dbh               = &connect_to_cache_db();

    # prepare SQL statements
    my $select_qid   = $dbh->prepare(qq{SELECT qid FROM queries WHERE corpus = ? AND casesensitivity = ? AND query = ?});
    my $insert_query = $dbh->prepare(qq{INSERT INTO queries (corpus, casesensitivity, query, time) VALUES (?, ?, ?, strftime('%s','now'))});
    my $update_query = $dbh->prepare(qq{UPDATE queries SET time = strftime('%s','now') WHERE qid = ?});

    foreach my $corpus ( @{ $config{"corpora"} } ) {
        $corpus_handles{$corpus} = new CWB::CL::Corpus $corpus;
    }
    my $corpus           = $config{"default_corpus"};
    my $corpus_handle    = $corpus_handles{$corpus};
    my $querymode        = "collo-word";
    my $case_sensitivity = 0;
    my $queryid          = 0;
    $cqp->exec($corpus);

    # mode (collo-word|collo-lemma|sentence)
    # case-sensitivity (yes|no)
    while ( my $queryref = <$socket> ) {
        $queryid++;
        chomp($queryref);
        $queryref =~ s/\s*$//;
        &log( "[$queryid] " . $queryref );

        # Switch corpus
        if ( $queryref =~ /^corpus ([\p{IsLu}_\d]+)$/ ) {
            if ( defined( $corpus_handles{$1} ) ) {
                $corpus        = $1;
                $corpus_handle = $corpus_handles{$corpus};
                $cqp->exec($corpus);
                &log("Switched corpus to '$corpus'");
            }
            else {
                &log("Unknown corpus '$corpus'");
            }
            next;
        }

        # Switch mode
        if ( $queryref =~ /^mode (collo-word|collo-lemma|sentence|collo)$/ ) {
            $querymode = $1;
            $querymode = "collo-word" if ( $querymode eq "collo" );
            &log("Switched query mode to '$querymode'");
            next;
        }

        # Switch case-sensitivity
        if ( $queryref =~ /^case-sensitivity (yes|no)$/ ) {
            if ( $1 eq "yes" ) {
                $case_sensitivity = 1;
                &log("Switched on case-sensitivity");
            }
            elsif ( $1 eq "no" ) {
                $case_sensitivity = 0;
                &log("Switched off case-sensitivity");
            }
            next;
        }

        # Perform query
        if ( $queryref =~ /^\[\[\{.*\}\]\]$/ ) {
            my $cache_handle;
            my $t0 = [&Time::HiRes::gettimeofday];
            my ( $t1, $t2, $t3 );
            my $cached;
            $dbh->do(qq{BEGIN EXCLUSIVE TRANSACTION});
            $select_qid->execute( $corpus, $case_sensitivity, $queryref );
            my $qids = $select_qid->fetchall_arrayref;
            my $qid;
	    my $query_times = "";

	    # query is not cached
            if ( @$qids == 0 ) {
                $cached = 0;
                $insert_query->execute( $corpus, $case_sensitivity, $queryref );
                $select_qid->execute( $corpus, $case_sensitivity, $queryref );
                $qids = $select_qid->fetchall_arrayref;
                $qid  = $qids->[0]->[0];
                open( $cache_handle, ">", File::Spec->catfile( $config{"cache_dir"}, $qid ) ) or die( "Can't open " . File::Spec->catfile( $config{"cache_dir"}, $qid ) . ": $!" );
                flock( $cache_handle, LOCK_EX );
                $dbh->do(qq{COMMIT});
                $t1 = [&Time::HiRes::gettimeofday];
                #&CWB::treebank::match_graph( $output, $cqp, $corpus_handle, $corpus, $querymode, $queryref, $case_sensitivity, $cache_handle );
                $query_times = sprintf " (%s + %s + %s)", &CWB::treebank::match_graph( $output, $cqp, $corpus_handle, $corpus, $querymode, $queryref, $case_sensitivity, $cache_handle );
                flock( $cache_handle, LOCK_UN );
                close($cache_handle) or die( "Can't open " . File::Spec->catfile( $config{"cache_dir"}, $qid ) . ": $!" );
            }

	    # query is cached
            else {
                $cached = 1;
                $qid    = $qids->[0]->[0];
                $update_query->execute($qid);
                $dbh->do(qq{COMMIT});
                $t1 = [&Time::HiRes::gettimeofday];
                open( $cache_handle, "<", File::Spec->catfile( $config{"cache_dir"}, $qid ) ) or die( "Can't open " . File::Spec->catfile( $config{"cache_dir"}, $qid ) . ": $!" );
                flock( $cache_handle, LOCK_SH );
                my ( $s_attributes, $p_attributes ) = &CWB::treebank::get_corpus_attributes($corpus_handle);

                while ( my $line = <$cache_handle> ) {
                    chomp($line);
		    my $stored = $json->decode($line);
                    my ( $sid, $result ) = @$stored;
                    print $output &CWB::treebank::transform_output( $s_attributes, $p_attributes, $querymode, $sid, $result ) . "\n";
                }
                flock( $cache_handle, LOCK_UN );
                close($cache_handle) or die( "Can't open " . File::Spec->catfile( $config{"cache_dir"}, $qid ) . ": $!" );
            }

            print $output "finito\n";
            $t2 = [&Time::HiRes::gettimeofday];
            &log( sprintf( "answered %s in %.3fs (%s, %.3f + %.3f%s)", $queryid, Time::HiRes::tv_interval( $t0, $t2 ), $cached ? "cached" : "not cached", Time::HiRes::tv_interval( $t0, $t1 ), Time::HiRes::tv_interval( $t1, $t2 ), $query_times ) );
            next;
        }

	# malformed query, ignore
        &log("ignored $queryid");
    }

    undef($cqp);
    undef $corpus_handles{$_} foreach ( keys %corpus_handles );
    undef($dbh);
}

sub connect_to_cache_db {
    my $cache_size = $config{"cache_size"};
    my $dbh = DBI->connect( "dbi:SQLite:dbname=" . File::Spec->catfile( $config{"cache_dir"}, $config{"cache_db"} ), "", "" ) or die("Cannot connect: $DBI::errstr");

    #$dbh->do(qq{SELECT icu_load_collation('en_GB', 'BE')});
    #$dbh->do(qq{PRAGMA foreign_keys = ON});
    $dbh->sqlite_create_function( "rmfile", 1, sub { unlink map( File::Spec->catfile( $config{"cache_dir"}, $_ ), @_ ); } );
    $dbh->do(
        qq{
CREATE TABLE IF NOT EXISTS queries (
    qid INTEGER PRIMARY KEY,
    corpus TEXT NOT NULL,
    casesensitivity INTEGER NOT NULL,
    query TEXT NOT NULL,
    time INTEGER NOT NULL,
    UNIQUE (corpus, casesensitivity, query)
)}
    );
    $dbh->do(
        qq{
CREATE TRIGGER IF NOT EXISTS limit_to_cache_size AFTER INSERT ON queries
    WHEN (SELECT count(*) FROM queries) > $cache_size
    BEGIN
        SELECT rmfile(qid) FROM queries WHERE qid IN (
            SELECT qid FROM queries ORDER BY time ASC LIMIT (
                SELECT count(*) - $cache_size FROM queries
            )
        );
        DELETE FROM queries WHERE qid IN (
            SELECT qid FROM queries ORDER BY time ASC LIMIT (
                SELECT count(*) - $cache_size FROM queries
            )
        );
    END
}
    );
    return $dbh;
}

sub kill_child {
    my ($child_pid) = @_;
    sleep(300);
    if ( defined( $child_processes{$child_pid} ) ) {
        &log("Sending SIGTERM to $child_pid");
        kill( &SIGTERM(), $child_pid );
    }
}

sub log {
    my ($string) = @_;
    my @abbr = qw( Jan Feb Mar Apr May Jun Jul Aug Sep Oct Nov Dec );
    my ( $sec, $min, $hour, $mday, $mon ) = (localtime)[ 0 .. 4 ];
    my $time = sprintf( "%s %02d %02d:%02d:%02d, %d", $abbr[$mon], $mday, $hour, $min, $sec, $$ );
    flock( $log, LOCK_EX ) or die "can't lock stdout: $!";
    print $log "[$time] $string\n";
    flock( $log, LOCK_UN ) or die "can't unlock stdout: $!";
}

END {
    close($server) if ( defined($server) );
    if ( defined($log) ) {
        &log("Shutdown $$");
        close($log) or die("Cannot close logfile: $!");
    }
}
