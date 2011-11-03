#!/usr/bin/perl

use warnings;
use strict;

use IO::Socket;
use POSIX qw(:sys_wait_h SIGTERM SIGKILL);
use Fcntl qw(:flock);
use threads;
use threads::shared;

use CWB::treebank;

use CWB::CQP;
use CWB::CL;

# read config
my %config = do "cwb-treebank_server.cfg";

# fork once, and let the parent exit
{
    my $pid = fork;
    exit if ($pid);
    die("Couldn't fork: $!") unless ( defined($pid) );    
}

# redirect STDERR
open(STDERR, ">>", $config{"logfile"}) or die("Can't reopen STDERR: $!");

# dissociate from the controlling terminal that started us and stop
# being part of whatever process group we had been a member of
POSIX::setsid() or die("Can't start a new session: $!");

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
print STDERR "foo\n";
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
    my ( $cqp, %corpus_handles );
    $SIG{INT} = $SIG{TERM} = $SIG{HUP} = sub { &log("Caught signal"); undef $cqp; undef $corpus_handles{$_} foreach ( keys %corpus_handles ); $socket->close(); exit; };
    $cqp = new CWB::CQP;
    $cqp->set_error_handler('die');
    $cqp->exec( "set Registry '" . $config{"registry"} . "'" );
    $CWB::CL::Registry = $config{"registry"};

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
        if ( $queryref =~ /^mode (collo-word|collo-lemma|sentence|collo)$/ ) {
            $querymode = $1;
            $querymode = "collo-word" if ( $querymode eq "collo" );
            &log("Switched query mode to '$querymode'");
            next;
        }
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
        if ( $queryref =~ /^\[\[\{.*\}\]\]$/ ) {
            my $result = &CWB::treebank::match_graph( $output, $cqp, $corpus_handle, $corpus, $querymode, $queryref, $case_sensitivity );
            print $output "finito\n";
            &log("answered $queryid");
            next;
        }
        &log("ignored $queryid");
    }
    undef($cqp);
    undef($corpus_handle);
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