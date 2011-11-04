#!/usr/bin/perl

use warnings;
use strict;
use open qw(:std :utf8);

use IO::Socket;
use JSON;
use Encode;

my $json = new JSON;

my $remote_host = "localhost";
my $remote_port = 5931;

my $socket = IO::Socket::INET->new(
    PeerAddr  => $remote_host,
    PeerPort  => $remote_port,
    Proto     => "tcp",
    ReuseAddr => 1,
    Timeout   => 5,
    Type      => SOCK_STREAM
) or die("Couldn't connect to $remote_host:$remote_port : $@\n");
binmode($socket, ":utf8");

# bachelor   amod
#       -       -
my $bachelor = $json->encode(
    [
	[ { "word" => "bachelor" }, { "relation" => "amod" } ],
	[ {}, {} ]
    ]
);

# give   iobj   dobj     -
#    -    PRP      -     -
#    -      -    NNS   det
#    -      -      -   the
my $give_me_the_creeps = $json->encode(
    [
	[ { "lemma" => "give" }, { "relation" => "iobj" }, { "relation" => "dobj" }, {} ],
	[ {}, { "pos" => "PRP" }, {}, {} ],
	[ {}, {}, { "pos" => "NNS" }, { "relation" => "det" } ],
	[ {}, {}, {}, { "word" => "the" } ]
    ]
);

#    -   iobj   dobj
#    -      -      -
#    -      -      -
my $ditransitive = $json->encode(
    [
	[{}, { "relation" => "iobj" }, { "relation" => "dobj" }],
	[{}, {}, {}],
	[{}, {}, {}]
    ]
);

my $monotransitive_give = $json->encode(
    [
	[{"lemma" => "give", "not_outdep" => ["iobj", "prep_to"]}, {"relation" => "dobj"}],
	[{}, {}]
    ]
);

my $he_not_as_subject = $json->encode(
    [
	[{"word" => "he", "not_indep" => ["nsubj"]}]
    ]
);

my $not_it_appears = $json->encode(
    [
	[{"word" => "appears"}, {"relation" => "nsubj"}],
	[{}, {"not_word" => "it"}]
    ]
);

my $should_be_empty = $json->encode(
    [
	[{"lemma" => "give", "not_outdep" => ["iobj", "prep_to"]}, { "relation" => "iobj" }, { "relation" => "dobj" }],
	[{}, {}, {}],
	[{}, {}, {}]
    ]
);

print $socket "corpus BNC_PARSED\n";
print $socket "mode sentence\n";
&match_graph($bachelor);
exit;
&match_graph($give_me_the_creeps);
&match_graph($ditransitive);
&match_graph($monotransitive_give);
&match_graph($he_not_as_subject);
&match_graph($not_it_appears);
&match_graph($should_be_empty);

close($socket);

sub beautify_json {
    my $in = shift;
    $json = $json->pretty(1);
    my $out = $json->encode($json->decode($in));
    $json = $json->pretty(0);
    return $out;
}

sub match_graph {
    my ($in_json) = @_;
    print $socket $in_json, "\n";
    while (my $out_json = <$socket>) {
	last if($out_json eq "finito\n");
	print decode("utf8", &beautify_json($out_json));
    }
}
