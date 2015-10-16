#!/usr/bin/perl

use warnings;
use strict;
use threads;
use threads::shared;

# roadmap
# 1.0: refactoring of handle_connection
use version; our $VERSION = qv('0.9.0');

use Carp;
use English qw( -no_match_vars );
use Fcntl qw(:flock);
use File::Spec;
use IO::Handle;
use IO::Socket;
use POSIX qw(:sys_wait_h SIGTERM SIGKILL);
use Time::HiRes;

use CWB::CQP;
use CWB::CL;
use DBI;
use JSON;

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
        open my $PIDFILE, ">", $pidfile or croak "Can't open $pidfile: $OS_ERROR";
        print {$PIDFILE} $pid or croak "Can't print to $pidfile: $OS_ERROR";
        close $PIDFILE or croak "Can't close $pidfile: $OS_ERROR";
        exit;
    }
    croak "Couldn't fork: $OS_ERROR" unless ( defined $pid );
}

# redirect STDERR, STDIN, STDOUT
open STDERR, ">>", $config{"logfile"}    or croak "Can't reopen STDERR: $OS_ERROR";
open STDIN,  "<",  File::Spec->devnull() or croak "Can't reopen STDIN: $OS_ERROR";
open STDOUT, ">",  File::Spec->devnull() or croak "Can't reopen STDOUT: $OS_ERROR";

# dissociate from the controlling terminal that started us and stop
# being part of whatever process group we had been a member of
POSIX::setsid() or croak "Can't start a new session: $OS_ERROR";

# clear file creation mask
umask 0;

# open logfile
open my $log, ">>", $config{"logfile"} or croak "Cannot open logfile: $OS_ERROR";

# make filehandle hot
$log->autoflush();

my $server_port = $config{"server_port"};
my $server      = IO::Socket::INET->new(
    LocalPort => $server_port,
    Type      => SOCK_STREAM,
    ReuseAddr => 1,
    Listen    => 10
) or croak "Couldn't be a tcp server on port $server_port : $EVAL_ERROR";

my $parent = $PROCESS_ID;
my %child_processes : shared;

# don't fear the reaper ;o)
# avoid zombies (Perl Cookbook, 16.9)
sub REAPER {
    while ( ( my $child_pid = waitpid -1, WNOHANG ) > 0 ) {
        lock(%child_processes);
        delete $child_processes{$child_pid};
    }
    local $SIG{CHLD} = \&REAPER;
    return;
}
local $SIG{CHLD} = \&REAPER;

my $time_to_die = 0;
local $SIG{INT} = local $SIG{TERM} = local $SIG{HUP} = sub { log_message("Caught signal (1)"); $time_to_die = 1; };

log_message("Hello, here's your server speaking. My pid is $PROCESS_ID");
log_message("Waiting for clients on port #$server_port.");
while ( not $time_to_die ) {
    while ( ( my $client = $server->accept ) ) {
        if ( not $config{"clients"}->{ $client->peerhost() } ) {
            log_message( sprintf "Ignored conncection from %s", $client->peerhost() );
            next;
        }
        log_message( sprintf "Accepted conncection from %s", $client->peerhost() );
        my $pid = fork;
        croak "fork: $OS_ERROR" unless defined $pid;
        if ( $pid == 0 ) {

            # we're the child
            handle_connection($client);
            exit;
        }
        else {

            # we're the parent
            $client->close();
            log_message("Forked child process $pid");
            {
                lock(%child_processes);
                $child_processes{$pid}++;
            }
            my $thread = threads->create( \&kill_child, $pid )->detach;
        }
    }
}

log_message('Time to die');

sub handle_connection {
    my $socket = shift;
    my $output = shift || $socket;
    my $json   = JSON->new();
    my ( $cqp, %corpus_handles, $dbh );
    local $SIG{INT} = local $SIG{TERM} = local $SIG{HUP} = sub { log_message('Caught signal (2)'); $socket->close(); undef $cqp; undef $corpus_handles{$_} foreach ( keys %corpus_handles ); undef $dbh; exit; };
    $cqp = CWB::CQP->new();
    $cqp->set_error_handler('die');
    $cqp->exec( q{set Registry '} . $config{'registry'} . q{'} );
    $CWB::CL::Registry = $config{'registry'};
    $dbh               = connect_to_cache_db();

    # prepare SQL statements
    my $select_qid   = $dbh->prepare(qq{SELECT qid FROM queries WHERE corpus = ? AND casesensitivity = ? AND query = ?});
    my $insert_query = $dbh->prepare(qq{INSERT INTO queries (corpus, casesensitivity, query, time) VALUES (?, ?, ?, strftime('%s','now'))});
    my $update_query = $dbh->prepare(qq{UPDATE queries SET time = strftime('%s','now') WHERE qid = ?});

    foreach my $corpus ( @{ $config{"corpora"} } ) {
        $corpus_handles{$corpus} = CWB::CL::Corpus->new($corpus);
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
        chomp $queryref;
        $queryref =~ s/\s*$//xms;
        log_message( "[$queryid] " . $queryref );

        # Switch corpus
        if ( $queryref =~ /^corpus[ ]([\p{IsLu}_\d]+)$/xms ) {
            if ( defined $corpus_handles{$1} ) {
                $corpus        = $1;
                $corpus_handle = $corpus_handles{$corpus};
                $cqp->exec($corpus);
                log_message("Switched corpus to '$corpus'");
            }
            else {
                log_message("Unknown corpus '$1'");
            }
            next;
        }

        # Switch mode
        if ( $queryref =~ /^mode[ ](collo-word|collo-lemma|sentence|collo|corpus-position|frequency)$/xms ) {
            $querymode = $1;
            $querymode = "collo-word" if ( $querymode eq "collo" );
            log_message("Switched query mode to '$querymode'");
            next;
        }

        # Switch case-sensitivity
        if ( $queryref =~ /^case-sensitivity[ ](yes|no)$/xms ) {
            if ( $1 eq "yes" ) {
                $case_sensitivity = 1;
                log_message("Switched on case-sensitivity");
            }
            elsif ( $1 eq "no" ) {
                $case_sensitivity = 0;
                log_message("Switched off case-sensitivity");
            }
            next;
        }

	# Perform frequency query
	if ( $querymode eq "frequency" and $queryref =~ /^ [[] [{] [^:]+ : [^:]+ [}] []] $/xms ) {
	    my ($t0, $t1);
	    $t0 = [ Time::HiRes::gettimeofday() ];
	    my $frequency = CWB::treebank::get_frequency( $cqp, $corpus_handle, $corpus, $queryref );
	    print {$output} $frequency . "\n" or croak "Can't print to socket: $OS_ERROR";
	    print {$output} "finito\n" or croak "Can't print to socket: $OS_ERROR";
	    $t1 = [ Time::HiRes::gettimeofday() ];
            log_message( sprintf "answered %s in %.3fs (%d)", $queryid, Time::HiRes::tv_interval( $t0, $t1 ), $frequency );
	}

        # Perform query
        if ( $queryref =~ /^ [[] [[] [{] .* [}] []] []] $/xms ) {
            my $cache_handle;
            my ( $t0, $t1, $t2 );
            $t0 = [ Time::HiRes::gettimeofday() ];
            my $cached;
            $dbh->do(qq{BEGIN EXCLUSIVE TRANSACTION});
            $select_qid->execute( $corpus, $case_sensitivity, $queryref );
            my $qids = $select_qid->fetchall_arrayref;
            my $qid;
            my $query_times = q{};

            # query is not cached
            if ( @{$qids} == 0 ) {
                $cached = 0;
                $insert_query->execute( $corpus, $case_sensitivity, $queryref );
                $select_qid->execute( $corpus, $case_sensitivity, $queryref );
                $qids = $select_qid->fetchall_arrayref;
                $qid  = $qids->[0]->[0];
                open $cache_handle, ">", File::Spec->catfile( $config{'cache_dir'}, $qid ) or croak "Can't open " . File::Spec->catfile( $config{'cache_dir'}, $qid ) . ": $OS_ERROR";
                flock $cache_handle, LOCK_EX;
                $dbh->do(qq{COMMIT});
                $t1 = [ Time::HiRes::gettimeofday() ];

                #&CWB::treebank::match_graph( $output, $cqp, $corpus_handle, $corpus, $querymode, $queryref, $case_sensitivity, $cache_handle );
                $query_times = sprintf " (%s + %s + %s)", CWB::treebank::match_graph( $output, $cqp, $corpus_handle, $corpus, $querymode, $queryref, $case_sensitivity, $cache_handle );
                flock $cache_handle, LOCK_UN;
                close $cache_handle or croak "Can't open " . File::Spec->catfile( $config{"cache_dir"}, $qid ) . ": $OS_ERROR";
            }

            # query is cached
            else {
                $cached = 1;
                $qid    = $qids->[0]->[0];
                $update_query->execute($qid);
                $dbh->do(qq{COMMIT});
                $t1 = [ Time::HiRes::gettimeofday() ];
                open $cache_handle, "<", File::Spec->catfile( $config{"cache_dir"}, $qid ) or croak "Can't open " . File::Spec->catfile( $config{"cache_dir"}, $qid ) . ": $OS_ERROR";
                flock $cache_handle, LOCK_SH;
                my ( $s_attributes, $p_attributes ) = CWB::treebank::get_corpus_attributes($corpus_handle);

                while ( my $line = <$cache_handle> ) {
                    chomp $line;
                    my $stored = $json->decode($line);
                    my ( $sid, $result ) = @{$stored};
                    print {$output} CWB::treebank::transform_output( $s_attributes, $p_attributes, $querymode, $sid, $result ) . "\n" or croak "Can't print to socket: $OS_ERROR";
                }
                flock $cache_handle, LOCK_UN;
                close $cache_handle or croak "Can't open " . File::Spec->catfile( $config{"cache_dir"}, $qid ) . ": $OS_ERROR";
            }

            print {$output} "finito\n" or croak "Can't print to socket: $OS_ERROR";
            $t2 = [ Time::HiRes::gettimeofday() ];
            log_message( sprintf "answered %s in %.3fs (%s, %.3f + %.3f%s)", $queryid, Time::HiRes::tv_interval( $t0, $t2 ), $cached ? "cached" : "not cached", Time::HiRes::tv_interval( $t0, $t1 ), Time::HiRes::tv_interval( $t1, $t2 ), $query_times );
            next;
        }

        # malformed query, ignore
        log_message("ignored $queryid");
    }

    undef $cqp;
    undef $corpus_handles{$_} foreach ( keys %corpus_handles );
    undef $dbh;
    return;
}

sub connect_to_cache_db {
    my $cache_size = $config{"cache_size"};
    my $dbh = DBI->connect( "dbi:SQLite:dbname=" . File::Spec->catfile( $config{"cache_dir"}, $config{"cache_db"} ), q{}, q{} ) or croak "Cannot connect: $DBI::errstr";

    #$dbh->do(qq{SELECT icu_load_collation('en_GB', 'BE')});
    #$dbh->do(qq{PRAGMA foreign_keys = ON});
    $dbh->sqlite_create_function(
        "rmfile", 1,
        sub {
            unlink map { File::Spec->catfile( $config{"cache_dir"}, $_ ) } @_;
        }
    );
    my $create_statement = <<'END_CREATE';
CREATE TABLE IF NOT EXISTS queries (
    qid INTEGER PRIMARY KEY,
    corpus TEXT NOT NULL,
    casesensitivity INTEGER NOT NULL,
    query TEXT NOT NULL,
    time INTEGER NOT NULL,
    UNIQUE (corpus, casesensitivity, query)
)
END_CREATE
    $dbh->do($create_statement);
    my $trigger_statement = <<"END_TRIGGER";
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
END_TRIGGER
    $dbh->do($trigger_statement);
    return $dbh;
}

sub kill_child {
    my ($child_pid) = @_;
    sleep 300;    # timeout
    if ( defined $child_processes{$child_pid} ) {
        log_message("Sending SIGTERM to $child_pid");
        kill SIGTERM, $child_pid;
    }
    return;
}

sub log_message {
    my ($string) = @_;
    my @abbr = qw( Jan Feb Mar Apr May Jun Jul Aug Sep Oct Nov Dec );
    my ( $sec, $min, $hour, $mday, $mon ) = localtime;
    my $time = sprintf "%s %02d %02d:%02d:%02d, %d", $abbr[$mon], $mday, $hour, $min, $sec, $PROCESS_ID;
    flock $log, LOCK_EX or croak "Can't lock stdout: $OS_ERROR";
    print {$log} "[$time] $string\n" or croak "Can't print to log: $OS_ERROR";
    flock $log, LOCK_UN or croak "Can't unlock stdout: $OS_ERROR";
    return;
}

END {
    close $server or croak "Can't close server: $OS_ERROR" if ( defined $server );
    if ( defined $log ) {
        log_message("Shutdown $PROCESS_ID");
        close $log or croak "Cannot close logfile: $OS_ERROR";
    }
}
