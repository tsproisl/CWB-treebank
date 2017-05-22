package CWB::treebank;

use warnings;
use strict;

# roadmap
# 1.0: heuristics for frequencies of query nodes
# 2.0: restrictions on order of nodes
use version; our $VERSION = qv('0.10.0');

use Carp;
use English qw( -no_match_vars );
use List::Util qw( min sum );
use List::MoreUtils qw( uniq );
use Storable qw( dclone );
use Time::HiRes;

use JSON;

use Data::Dumper;

sub match_graph {
    my ( $output, $cqp, $corpus_handle, $registry_handle, $corpus, $querymode, $queryref, $case_sensitivity, $cache_handle ) = @_;
    my $json  = JSON->new();
    my $query = decode_json($queryref);
    $cqp->exec($corpus);
    my ( $s_attributes, $p_attributes ) = get_corpus_attributes($corpus_handle, $registry_handle);

    # cancel if there are no restrictions
    my $local_queryref = $queryref;
    $local_queryref =~ s/"(not_)?(relation|word|lower|pos|lemma|wc|indep|outdep)"://gxms;
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
    execute_query( $cqp, $s_attributes->{"s_id"}, $s_attributes->{"s_ignore"}, $query, \%ids, \@corpus_order, $case_sensitivity, \%frequencies, $p_attributes );

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
    my ( $cqp, $corpus_handle, $registry_handle, $corpus, $queryref ) = @_;
    my ( $s_attributes, $p_attributes ) = get_corpus_attributes($corpus_handle, $registry_handle);
    my $corpus_size = $p_attributes->{"word"}->max_cpos;
    my $frequency = 0;
    my $query = decode_json($queryref);
    my @attributes = grep { /^(not_)?(word|lower|pos|lemma|wc)$/ } keys %{$query->[0]};
    if (scalar @attributes == 0) {
	$frequency = $corpus_size;
    }
    elsif (scalar @attributes == 1) {
	$attributes[0] =~ /^(not_)?(word|lower|pos|lemma|wc)$/;
	my $negated = $1 ? 1 : 0;
	my $attribute = $2;
	my @values = split /[|]/xms, $query->[0]->{$attributes[0]};
	for my $value (@values) {
	    my $value_id = $p_attributes->{$attribute}->str2id($value);
	    my $value_freq = $p_attributes->{$attribute}->id2freq($value_id);
	    $frequency += $value_freq;
	}
	if ($negated) {
	    $frequency = $corpus_size - $frequency;
	}
    }
    return $frequency;
}

sub get_multiple_frequencies {
    my ( $cqp, $corpus_handle, $registry_handle, $corpus, $attribute, $queryref ) = @_;
    my ( $s_attributes, $p_attributes ) = get_corpus_attributes($corpus_handle, $registry_handle);
    my %frequencies;
    my $query = decode_json($queryref);
    my $nr_of_items = scalar @$query;
    for my $item (@$query) {
	my $item_id = $p_attributes->{$attribute}->str2id($item);
	my $item_freq = $p_attributes->{$attribute}->id2freq($item_id);
	$frequencies{$item} = $item_freq;
    }
    return encode_json( \%frequencies ), $nr_of_items;
}

sub transform_output {
    my ( $s_attributes, $p_attributes, $querymode, $sid, $result ) = @_;
    if ( $sid eq q{} and @{$result} == 0 ) {
        return encode_json( {} );
    }
    my ( $start, $end ) = $s_attributes->{"s_id"}->struc2cpos($sid);
    my @positions = uniq(map { @{$_} } @{$result});

    if ( $querymode =~ /^collo-(word|lower|lemma)$/xms ) {
	my $attribute = $1;
	my $forms = {};
	foreach my $position (@positions) {
	    $forms->{$position - $start} = $p_attributes->{$attribute}->cpos2str($position);
	}
        return encode_json(
            {   "s_id"          => $s_attributes->{"s_id"}->struc2str($sid),
                "s_text" => $s_attributes->{"s_text"}->struc2str($sid),
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
                "s_text" => $s_attributes->{"s_text"}->struc2str($sid),
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
    my $corpus_size = $p_attributes->{"word"}->max_cpos;
    foreach my $i ( 0 .. $#{$query} ) {
        my $token = $query->[$i]->[$i];

        # for key in token:
        # if in p_attributes, then sum map {id2freq} regex2id (size - result if negated)
        my $frequency = $corpus_size;
        my @freqs;
        foreach my $att ( grep { $p_attributes->{$_} } keys %{$token} ) {
            my @ids = $p_attributes->{$att}->regex2id( $token->{$att} );
            if ( scalar @ids > 0 ) {
                push @freqs, sum( map { $p_attributes->{$att}->id2freq($_) } @ids );
            }
        }
        if ( scalar @freqs > 0 ) {
            $frequency = min @freqs;
        }
        else {
            # Note: Try out adding indeps and outdeps information
            my $querystring = join ' & ', map { map_token_restrictions( $_, $token->{$_}, $case_sensitivity, $p_attributes ) } keys %{$token};

            #my $querystring = &build_query($query, $i, $case_sensitivity); # inefficient
            if ($querystring) {
                $cqp->exec("A = [$querystring]");
                ($frequency) = $cqp->exec("size A");
            }
        }
        $frequencies{$i} = $frequency;
    }
    return %frequencies;
}

sub execute_query {
    my ( $cqp, $s_id, $s_ignore, $query, $ids_ref, $corpus_order_ref, $case_sensitivity, $frequencies_ref, $p_attributes ) = @_;
    foreach my $i ( sort { $frequencies_ref->{$a} <=> $frequencies_ref->{$b} } keys %{$frequencies_ref} ) {
        @{$corpus_order_ref} = ();
        my $querystring = build_query( $query, $i, $case_sensitivity, $p_attributes );

	# warn $querystring, "\n";
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

sub get_outdep_attribute {
    my ( $relation, $p_attributes ) = @_;
    if ( $relation =~ m/^([^:]+)(?::.+)?$/xms ) {
	my $outdep = "out_$1";
        if ( $p_attributes->{$outdep} ) {
            return $outdep;
        }
        else {
            croak "Unknown outdep attribute: $relation, $outdep";
        }
    }
    else {
        croak "Cannot determine outdep attribute: $relation";
    }
}

sub get_outdeps {
    my ( $cpos, $p_attributes ) = @_;
    my @outdeps;
    foreach my $attribute ( keys %{$p_attributes} ) {
        if ( substr( $attribute, 0, 4 ) eq "out_" ) {
            my $outdeps = $p_attributes->{$attribute}->cpos2str($cpos);
            $outdeps =~ s/^[|]//xms;
            $outdeps =~ s/[|]$//xms;
            push @outdeps, split( /[|]/xms, $outdeps );
        }
    }
    return \@outdeps;
}

sub build_query {
    my ( $query, $i, $case_sensitivity, $p_attributes ) = @_;
    my $token = $query->[$i]->[$i];
    my @querystring;

    my $tokrestr = join ' & ', map { map_token_restrictions( $_, $token->{$_}, $case_sensitivity, $p_attributes ) } keys %{$token};
    push @querystring, $tokrestr if ($tokrestr);

    my $indeps = join ' & ', map {
        '('
            . join(
            ' | ',              map { '(indep contains "' . $_ . '\(.*")' }
                # split /[|]/xms, $_
		@{$_}
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
            ' | ',              map { '(' . get_outdep_attribute($_, $p_attributes) . ' contains "' . $_ . '\(.*")' }
                # split /[|]/xms, $_
		@{$_}
            )
            . ')'
        } grep {defined}
        map { $query->[$i]->[$_]->{'relation'} } ( 0 .. $#{$query} );
    # $outdeps .= ' & (ambiguity(outdep) >= ' . scalar( grep {defined} map { $query->[$i]->[$_]->{'relation'} } ( 0 .. $#{$query} ) ) . ')' if ($outdeps);
    push @querystring, $outdeps if ($outdeps);

    my $querystring = join ' & ', @querystring;
    return $querystring;
}

sub map_token_restrictions {
    my ( $key, $value, $case_sensitivity, $p_attributes ) = @_;
    if ( $key eq "not_indep" ) {
        return join ' & ', map { '(indep not contains "' . $_ . '\(.*")' } @{$value};
    }
    elsif ( $key eq "not_outdep" ) {
        return join ' & ', map { '(' . get_outdep_attribute($_, $p_attributes) . ' not contains "' . $_ . '\(.*")' } @{$value};
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
        if ( $query->[$i]->[$query_node]->{"relation"} ) {
            $query_incoming_rels[$i] = join '|', @{$query->[$i]->[$query_node]->{"relation"}};
        }
    }
    $number_of_incoming_rels = grep {defined} @query_incoming_rels;

    # collect outgoing dependency relations for query_node in query graph
    my ( $number_of_outgoing_rels, @query_outgoing_rels );
    foreach my $i ( 0 .. $#{$query} ) {
        if ( $query->[$query_node]->[$i]->{"relation"} ) {
            $query_outgoing_rels[$i] = join '|', @{$query->[$query_node]->[$i]->{"relation"}};
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
        my @outdeps = @{ get_outdeps( $cpos, $p_attributes ) };
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
    my ( $corpus_handle, $registry_handle ) = @_;
    my %s_attributes;
    $s_attributes{"sentence"} = $corpus_handle->attribute( "s", "s" );
    my @s_attribute_list = qw( s_id s_text s_ignore );
    foreach my $attribute (@s_attribute_list) {
        $s_attributes{$attribute} = $corpus_handle->attribute( $attribute, "s" );
    }
    my %p_attributes;
    my @p_attribute_list = $registry_handle->list_attributes("p");
    foreach my $attribute (@p_attribute_list) {
        $p_attributes{$attribute} = $corpus_handle->attribute( $attribute, "p" );
    }
    return ( \%s_attributes, \%p_attributes );
}

1;
