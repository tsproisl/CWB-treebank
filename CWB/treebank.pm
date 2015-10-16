package CWB::treebank;

use warnings;
use strict;

# roadmap
# 1.0: heuristics for frequencies of query nodes
# 2.0: restrictions on order of nodes
use version; our $VERSION = qv('0.9.0');

use Carp;
use English qw( -no_match_vars );
use List::Util qw(sum);
use List::MoreUtils qw( uniq );
use Storable qw( dclone );
use Time::HiRes;

use JSON;

sub match_graph {
    my ( $output, $cqp, $corpus_handle, $corpus, $querymode, $queryref, $case_sensitivity, $cache_handle ) = @_;
    my $json  = JSON->new();
    my $query = decode_json($queryref);
    $cqp->exec($corpus);
    my ( $s_attributes, $p_attributes ) = get_corpus_attributes($corpus_handle);

    # cancel if there are no restrictions
    my $local_queryref = $queryref;
    $local_queryref =~ s/"(not_)?(relation|word|pos|lemma|wc|indep|outdep)"://gxms;
    $local_queryref =~ s/"[.][*+?]"//gxms;
    if ( $local_queryref =~ m/^[][}{, ]*$/xms ) {
        print {$cache_handle} $json->encode( [ q{}, [] ] ) . "\n" or croak "Can't print to cache: $OS_ERROR";
        return 0, 0, 0;
    }

    my $t0 = [ Time::HiRes::gettimeofday() ];

    # check frequencies
    my %frequencies = check_frequencies( $cqp, $query, $case_sensitivity, $p_attributes );

    my $t1 = [ Time::HiRes::gettimeofday() ];

    # execute query
    my %ids;
    my @corpus_order;
    execute_query( $cqp, $s_attributes->{"s_id"}, $s_attributes->{"s_ignore"}, $query, \%ids, \@corpus_order, $case_sensitivity, \%frequencies );

    my $t2 = [ Time::HiRes::gettimeofday() ];

    # match
    foreach my $sid (@corpus_order) {
        my $candidates_ref = dclone $ids{$sid};
        my $depth          = 0;

        #my %depth_to_query_node = map { $_ => $_ } ( 0 .. $#$query );
        my $count_relations = sub {
            my $n = shift;
            my $relations = scalar grep { defined $query->[$_]->[$n] } ( 0 .. $#{$query} );
            $relations += scalar grep { defined $query->[$n]->[$_] } ( 0 .. $#{$query} );
        };
        my @sorted_indexes = reverse sort { $count_relations->($a) <=> $count_relations->($b) } ( 0 .. $#{$query} );
        my %depth_to_query_node = map { $_ => $sorted_indexes[$_] } ( 0 .. $#{$query} );
        my $result = match( $depth, $query, $p_attributes, \%depth_to_query_node, $candidates_ref );
        if ( defined $result ) {
            print {$cache_handle} $json->encode( [ $sid, $result ] ) . "\n" or croak "Can't print to cache: $OS_ERROR";
            print {$output} transform_output( $s_attributes, $p_attributes, $querymode, $sid, $result ) . "\n" or croak "Can't print to socket: $OS_ERROR";
        }
    }
    my $t3 = [ Time::HiRes::gettimeofday() ];
    return Time::HiRes::tv_interval( $t0, $t1 ), Time::HiRes::tv_interval( $t1, $t2 ), Time::HiRes::tv_interval( $t2, $t3 );
}

sub get_frequency {
    my ( $cqp, $corpus_handle, $corpus, $queryref ) = @_;
    my ( $s_attributes, $p_attributes ) = get_corpus_attributes($corpus_handle);
    my $frequency = 0;
    my $query = decode_json($queryref);
    my @attributes = keys %{$query->[0]};
    if (scalar @attributes == 0) {
	$frequency = $p_attributes->{"word"}->max_cpos;
    }
    elsif (scalar @attributes == 1) {
	my $attribute = $attributes[0];
	my @values = split /[|]/xms, (values %{$query->[0]})[0];
	for my $value (@values) {
	    my $value_id = $p_attributes->{$attribute}->str2id($value);
	    my $value_freq = $p_attributes->{$attribute}->id2freq($value_id);
	    $frequency += $value_freq;
	}
    }
    return $frequency;
}

sub transform_output {
    my ( $s_attributes, $p_attributes, $querymode, $sid, $result ) = @_;
    if ( $sid eq q{} and @{$result} == 0 ) {
        return encode_json( {} );
    }
    my ( $start, $end ) = $s_attributes->{"s_id"}->struc2cpos($sid);
    my @positions = uniq(map { @{$_} } @{$result});

    if ( $querymode eq "collo-word" ) {
	my $forms = {};
	foreach my $position (@positions) {
	    $forms->{$position - $start} = $p_attributes->{"word"}->cpos2str($position);
	}
        return encode_json(
            {   "s_id"          => $s_attributes->{"s_id"}->struc2str($sid),
                "s_original_id" => $s_attributes->{"s_original_id"}->struc2str($sid),
                "forms"         => $forms,
                "tokens"        => [
                    map {
                        [ map { $_ - $start } @{$_} ]
                        } @{$result}
                ]
            }
        );
    }
    elsif ( $querymode eq "collo-lemma" ) {
	my $forms = {};
	foreach my $position (@positions) {
	    $forms->{$position - $start} = $p_attributes->{"lemma"}->cpos2str($position);
	}
        return encode_json(
            {   "s_id"          => $s_attributes->{"s_id"}->struc2str($sid),
                "s_original_id" => $s_attributes->{"s_original_id"}->struc2str($sid),
                "forms"         => $forms,
                "tokens"        => [
                    map {
                        [ map { $_ - $start } @{$_} ]
                        } @{$result}
                ]
            }
        );
    }
    elsif ( $querymode eq "sentence" ) {
        return encode_json(
            {   "s_id"          => $s_attributes->{"s_id"}->struc2str($sid),
                "s_original_id" => $s_attributes->{"s_original_id"}->struc2str($sid),
                "sentence"      => [ $p_attributes->{"word"}->cpos2str( $start .. $end ) ],
                "tokens"        => [
                    map {
                        [ map { $_ - $start } @{$_} ]
                        } @{$result}
                ]
            }
        );
    }
    elsif ( $querymode eq "corpus-position" ) {
        return encode_json(
            {   "s_start" => $start,
                "s_end"   => $end,
                "tokens"  => [
                    map {
                        [ map { $_ - $start } @{$_} ]
                        } @{$result}
                ]
            }
        );
    }
}

sub check_frequencies {
    my ( $cqp, $query, $case_sensitivity, $p_attributes ) = @_;
    my %frequencies;
    foreach my $i ( 0 .. $#{$query} ) {
        my $token = $query->[$i]->[$i];

        # Note: Try out adding indeps and outdeps information
        my $querystring = join ' & ', map { map_token_restrictions( $_, $token->{$_}, $case_sensitivity ) } keys %{$token};

        #my $querystring = &build_query($query, $i, $case_sensitivity); # inefficient
        if ($querystring) {
            $cqp->exec("A = [$querystring]");
            ( $frequencies{$i} ) = $cqp->exec("size A");
        }
        else {
            $frequencies{$i} = $p_attributes->{"word"}->max_cpos;

            #die("It seems that the input graph is not connected. Is that right?\n");
        }
    }
    return %frequencies;
}

sub execute_query {
    my ( $cqp, $s_id, $s_ignore, $query, $ids_ref, $corpus_order_ref, $case_sensitivity, $frequencies_ref ) = @_;
    foreach my $i ( sort { $frequencies_ref->{$a} <=> $frequencies_ref->{$b} } keys %{$frequencies_ref} ) {
        @{$corpus_order_ref} = ();
        my $querystring = build_query( $query, $i, $case_sensitivity );

        # print $querystring, "\n";
        # my $t0 = [ Time::HiRes::gettimeofday() ];
        $cqp->exec("[$querystring]");
        if ( ( $cqp->exec("size Last") )[0] > 0 ) {
            foreach my $match ( $cqp->exec("tabulate Last match") ) {
                next if ( defined $s_ignore && substr( $s_ignore->cpos2str($match), 0, 3 ) eq 'yes' );
                my $sid = $s_id->cpos2struc($match);
                if ( !defined $ids_ref->{$sid}->[$i] ) {
                    push @{$corpus_order_ref}, $sid;
                }
                push @{ $ids_ref->{$sid}->[$i] }, $match;
            }
        }

        # my $t1 = [ Time::HiRes::gettimeofday() ];
        # print Time::HiRes::tv_interval($t0, $t1), "\n";
        $cqp->exec("Last expand to s");
        $cqp->exec("Last");
    }
    return;
}

sub build_query {
    my ( $query, $i, $case_sensitivity ) = @_;
    my $token = $query->[$i]->[$i];
    my @querystring;

    my $tokrestr = join ' & ', map { map_token_restrictions( $_, $token->{$_}, $case_sensitivity ) } keys %{$token};
    push @querystring, $tokrestr if ($tokrestr);

    my $indeps = join ' & ', map {
        '('
            . join(
            ' | ',              map { '(indep contains "' . $_ . '\(.*")' }
                split /[|]/xms, $_
            )
            . ')'
        }
        grep {defined}
        map { $query->[$_]->[$i]->{'relation'} } ( 0 .. $#{$query} );
    $indeps .= ' & (ambiguity(indep) >= ' . scalar( grep {defined} map { $query->[$_]->[$i]->{'relation'} } ( 0 .. $#{$query} ) ) . ')' if ($indeps);
    push @querystring, $indeps if ($indeps);

    my $outdeps = join ' & ', map {
        '('
            . join(
            ' | ',              map { '(outdep contains "' . $_ . '\(.*")' }
                split /[|]/xms, $_
            )
            . ')'
        } grep {defined}
        map { $query->[$i]->[$_]->{'relation'} } ( 0 .. $#{$query} );
    $outdeps .= ' & (ambiguity(outdep) >= ' . scalar( grep {defined} map { $query->[$i]->[$_]->{'relation'} } ( 0 .. $#{$query} ) ) . ')' if ($outdeps);
    push @querystring, $outdeps if ($outdeps);

    my $querystring = join ' & ', @querystring;
    return $querystring;
}

sub map_token_restrictions {
    my ( $key, $value, $case_sensitivity ) = @_;
    if ( $key =~ m/^not_((?:in|out)dep)$/xms ) {
        return join ' & ', map { '(' . $1 . ' not contains "' . $_ . '\(.*")' } @{$value};
    }
    elsif ( substr( $key, 0, 4 ) eq "not_" ) {
        $key = substr $key, 4;
        return qq{($key != "$value"} . ignore_case( $key, $case_sensitivity ) . ')';
    }
    else {
        return qq{($key = "$value"} . ignore_case( $key, $case_sensitivity ) . ')';
    }
}

sub intersection {
    my ( $array1, $array2 ) = @_;
    my %count = ();
    my @intersection;
    foreach my $element ( @{$array1}, @{$array2} ) {
        $count{$element}++;
    }
    foreach my $element ( keys %count ) {
        push @intersection, $element if ( $count{$element} > 1 );
    }
    return \@intersection;
}

sub ignore_case {
    my ( $key, $case_sensitivity ) = @_;
    return ( ( $key eq "word" or $key eq "lemma" ) and not $case_sensitivity ) ? ' %c' : q{};
}

sub match {
    my ( $depth, $query, $p_attributes, $depth_to_query_node_ref, $candidates ) = @_;
    my $query_node = $depth_to_query_node_ref->{$depth};
    my $result;

    # if matching has been successful, return matching corpus
    # positions
    if ( $depth > $#{$query} ) {
        @{$candidates} = map { $_->[0] } @{$candidates};
        return [$candidates];
    }

    # collect incoming dependency relations for query_node in query graph
    my ( $number_of_incoming_rels, @query_incoming_rels );
    foreach my $i ( 0 .. $#{$query} ) {
        if ( $query->[$i]->[$query_node] ) {
            $query_incoming_rels[$i] = $query->[$i]->[$query_node]->{"relation"};
        }
    }
    $number_of_incoming_rels = grep {defined} @query_incoming_rels;

    # collect outgoing dependency relations for query_node in query graph
    my ( $number_of_outgoing_rels, @query_outgoing_rels );
    foreach my $i ( 0 .. $#{$query} ) {
        if ( $query->[$query_node]->[$i] ) {
            $query_outgoing_rels[$i] = $query->[$query_node]->[$i]->{"relation"};
        }
    }
    $number_of_outgoing_rels = grep {defined} @query_outgoing_rels;

CPOS:
    foreach my $cpos ( @{ $candidates->[$query_node] } ) {

        # store corpus position for current query_node, remove it from
        # other nodes
        my $local_candidates;
        for my $query_node ( 0 .. $#{$candidates} ) {
            $local_candidates->[$query_node] = [ grep { $_ != $cpos } @{ $candidates->[$query_node] } ];
        }
        $local_candidates->[$query_node] = [$cpos];

        # collect incoming dependency relations for corpus position
        my ( $indeps, @indeps );
        $indeps = $p_attributes->{"indep"}->cpos2str($cpos);
        $indeps =~ s/^[|]//xms;
        $indeps =~ s/[|]$//xms;
        @indeps = split /[|]/xms, $indeps;
        next CPOS if ( @indeps < $number_of_incoming_rels );

        # collect outgoing dependency relations for corpus position
        my ( $outdeps, @outdeps );
        $outdeps = $p_attributes->{"outdep"}->cpos2str($cpos);
        $outdeps =~ s/^[|]//xms;
        $outdeps =~ s/[|]$//xms;
        @outdeps = split /[|]/xms, $outdeps;
        next CPOS if ( @outdeps < $number_of_outgoing_rels );

        # collect corpus position of the start nodes of the incoming
        # dependency relations
        my @corpus_candidates;
        foreach my $i ( 0 .. $#query_incoming_rels ) {
            my $rel = $query_incoming_rels[$i];
            next unless ( defined $rel );
            foreach my $indep (@indeps) {
                if ( $indep =~ m/^ (?<relation>$rel) [(] (?<offset>-?\d+) (?:')* ,/xms ) {
                    my $offset     = $LAST_PAREN_MATCH{"offset"};
                    my $start_cpos = $cpos + $offset;
                    push @{ $corpus_candidates[$i] }, $start_cpos;
                }
            }
            @{ $corpus_candidates[$i] } = uniq @{ $corpus_candidates[$i] };
        }

        # collect corpus position of the start nodes of the outgoing
        # dependency relations
        foreach my $i ( 0 .. $#query_outgoing_rels ) {
            my $rel = $query_outgoing_rels[$i];
            next unless ( defined $rel );
            foreach my $outdep (@outdeps) {
                if ( $outdep =~ m/^ (?<relation>$rel) [(]0 (?:')* , (?<offset>-?\d+) (?:')* [)] $/xms ) {
                    my $offset     = $LAST_PAREN_MATCH{"offset"};
                    my $start_cpos = $cpos + $offset;
                    push @{ $corpus_candidates[$i] }, $start_cpos;
                }
            }
            @{ $corpus_candidates[$i] } = uniq @{ $corpus_candidates[$i] };
        }

        # intersect candidate corpus positions for connected nodes
        # with those corpus positions that are actually connected to
        # the current node
        foreach my $i ( 0 .. $#{$local_candidates} ) {
            next if ( !defined $corpus_candidates[$i] );
            $local_candidates->[$i] = intersection( $local_candidates->[$i], $corpus_candidates[$i] );
            next CPOS if ( @{ $local_candidates->[$i] } == 0 );
        }

        # recursion
        my $local_result = match( $depth + 1, $query, $p_attributes, $depth_to_query_node_ref, $local_candidates );
        if ( defined $local_result ) {
            push @{$result}, @{$local_result};
        }
    }

    # not matching, return undef
    return $result;
}

sub get_corpus_attributes {
    my ($corpus_handle) = @_;
    my %s_attributes;
    $s_attributes{"sentence"}      = $corpus_handle->attribute( "s",             "s" );
    $s_attributes{"s_id"}          = $corpus_handle->attribute( "s_id",          "s" );
    $s_attributes{"s_original_id"} = $corpus_handle->attribute( "s_original_id", "s" );
    $s_attributes{"s_ignore"}      = $corpus_handle->attribute( "s_ignore",      "s" );
    my %p_attributes;
    $p_attributes{"word"}   = $corpus_handle->attribute( "word",   "p" );
    $p_attributes{"pos"}    = $corpus_handle->attribute( "pos",    "p" );
    $p_attributes{"lemma"}  = $corpus_handle->attribute( "lemma",  "p" );
    $p_attributes{"wc"}     = $corpus_handle->attribute( "wc",     "p" );
    $p_attributes{"indep"}  = $corpus_handle->attribute( "indep",  "p" );
    $p_attributes{"outdep"} = $corpus_handle->attribute( "outdep", "p" );
    return ( \%s_attributes, \%p_attributes );
}

1;
