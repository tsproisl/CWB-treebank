package CWB::treebank;

use warnings;
use strict;

use Storable qw( dclone );
use JSON;
use Data::Dumper;
use List::Util qw(sum);
use List::MoreUtils qw( uniq );

use Time::HiRes;

sub match_graph {
    my ( $output, $cqp, $corpus_handle, $corpus, $querymode, $queryref, $case_sensitivity, $cache_handle ) = @_;
    my $json = new JSON;
    local $Data::Dumper::Indent = 0;
    local $Data::Dumper::Purity = 1;
    my $query = decode_json($queryref);
    my @result;
    $cqp->exec($corpus);
    my ( $s_attributes, $p_attributes ) = &get_corpus_attributes($corpus_handle);

    # cancel if there are no restrictions
    my $local_queryref = $queryref;
    $local_queryref =~ s/"(not_)?(relation|word|pos|lemma|wc|indep|outdep)"://g;
    $local_queryref =~ s/"\.[*+?]"//g;
    if ( $local_queryref =~ m/^[][}{, ]*$/ ) {
        print $cache_handle $json->encode( [ "", [] ] ) . "\n";
        return;
    }

    my $t0 = [ Time::HiRes::gettimeofday() ];

    # check frequencies
    my %frequencies = &check_frequencies( $cqp, $query, $case_sensitivity, $p_attributes );

    my $t1 = [ Time::HiRes::gettimeofday() ];

    # execute query
    my %ids;
    my @corpus_order;
    &execute_query( $cqp, $s_attributes->{"s_id"}, $query, \%ids, \@corpus_order, $case_sensitivity, \%frequencies );

    my $t2 = [ Time::HiRes::gettimeofday() ];

    # match
    foreach my $sid (@corpus_order) {
        my $candidates_ref      = dclone $ids{$sid};
        my $depth               = 0;
        my %depth_to_query_node = map { $_ => $_ } ( 0 .. $#$query );
        my $result              = &match( $depth, $query, $p_attributes, \%depth_to_query_node, $candidates_ref );
        if ( defined $result ) {
            print $cache_handle $json->encode( [ $sid, $result ] ) . "\n";
            print $output &transform_output( $s_attributes, $p_attributes, $querymode, $sid, $result ) . "\n";
        }
    }
    my $t3 = [ Time::HiRes::gettimeofday() ];
    return Time::HiRes::tv_interval( $t0, $t1 ), Time::HiRes::tv_interval( $t1, $t2 ), Time::HiRes::tv_interval( $t2, $t3 );
}

sub transform_output {
    my ( $s_attributes, $p_attributes, $querymode, $sid, $result ) = @_;
    if ( $sid eq "" and @$result == 0 ) {
        return encode_json( {} );
    }
    my ( $start, $end ) = $s_attributes->{"s_id"}->struc2cpos($sid);

    if ( $querymode eq "collo-word" ) {
        return encode_json(
            {   "s_id"          => $s_attributes->{"s_id"}->struc2str($sid),
                "s_original_id" => $s_attributes->{"s_original_id"}->struc2str($sid),
                "tokens"        => [
                    map {
                        [ map { $p_attributes->{"word"}->cpos2str($_) } @$_ ]
                        } @$result
                ]
            }
        );
    }
    elsif ( $querymode eq "collo-lemma" ) {
        return encode_json(
            {   "s_id"          => $s_attributes->{"s_id"}->struc2str($sid),
                "s_original_id" => $s_attributes->{"s_original_id"}->struc2str($sid),
                "tokens"        => [
                    map {
                        [ map { $p_attributes->{"lemma"}->cpos2str($_) } @$_ ]
                        } @$result
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
                        [ map { $_ - $start } @$_ ]
                        } @$result
                ]
            }
        );
    }
}

sub check_frequencies {
    my ( $cqp, $query, $case_sensitivity, $p_attributes ) = @_;
    my %frequencies;
    for ( my $i = 0; $i <= $#$query; $i++ ) {
        my $token = $query->[$i]->[$i];

        # Note: Try out adding indeps and outdeps information
        my $querystring = join( " & ", map( &map_token_restrictions( $_, $token->{$_}, $case_sensitivity ), keys %$token ) );

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
    my ( $cqp, $s_id, $query, $ids_ref, $corpus_order_ref, $case_sensitivity, $frequencies_ref ) = @_;
    foreach my $i ( sort { $frequencies_ref->{$a} <=> $frequencies_ref->{$b} } keys %{$frequencies_ref} ) {
        @$corpus_order_ref = ();
        my $querystring = &build_query( $query, $i, $case_sensitivity );

        # print $querystring, "\n";
        # my $t0 = [ Time::HiRes::gettimeofday() ];
        $cqp->exec("[$querystring]");
        if ( ( $cqp->exec("size Last") )[0] > 0 ) {
            foreach my $match ( $cqp->exec("tabulate Last match") ) {
                my $sid = $s_id->cpos2struc($match);
                if ( !defined $ids_ref->{$sid}->[$i] ) {
                    push @$corpus_order_ref, $sid;
                }
                push @{ $ids_ref->{$sid}->[$i] }, $match;
            }
        }

        # my $t1 = [ Time::HiRes::gettimeofday() ];
        # print Time::HiRes::tv_interval($t0, $t1), "\n";
        $cqp->exec("Last expand to s");
        $cqp->exec("Last");
    }
}

sub build_query {
    my ( $query, $i, $case_sensitivity ) = @_;
    my $token = $query->[$i]->[$i];
    my @querystring;

    my $tokrestr = join( " & ", map( &map_token_restrictions( $_, $token->{$_}, $case_sensitivity ), keys %$token ) );
    push( @querystring, $tokrestr ) if ($tokrestr);

    my $indeps = join( ' & ', map { '(' . join( ' | ', map( '(indep contains "' . $_ . '\(.*")', split( /\|/, $_ ) ) ) . ')' } grep( defined, map( $query->[$_]->[$i]->{"relation"}, ( 0 .. $#$query ) ) ) );
    $indeps .= ' & (ambiguity(indep) >= ' . scalar( grep( defined, map( $query->[$_]->[$i]->{"relation"}, ( 0 .. $#$query ) ) ) ) . ')' if ($indeps);
    push( @querystring, $indeps ) if ($indeps);

    my $outdeps = join( ' & ', map { '(' . join( ' | ', map( '(outdep contains "' . $_ . '\(.*")', split( /\|/, $_ ) ) ) . ')' } grep( defined, map( $query->[$i]->[$_]->{"relation"}, ( 0 .. $#$query ) ) ) );
    $outdeps .= ' & (ambiguity(outdep) >= ' . scalar( grep( defined, map( $query->[$i]->[$_]->{"relation"}, ( 0 .. $#$query ) ) ) ) . ')' if ($outdeps);
    push( @querystring, $outdeps ) if ($outdeps);

    my $querystring = join( ' & ', @querystring );
    return $querystring;
}

sub map_token_restrictions {
    my ( $key, $value, $case_sensitivity ) = @_;
    if ( $key =~ m/^not_((in|out)dep)$/ ) {
        return join( ' & ', map( '(' . $1 . ' not contains "' . $_ . '\(.*")', @$value ) );
    }
    elsif ( substr( $key, 0, 4 ) eq "not_" ) {
        $key = substr( $key, 4 );
        return '(' . $key . ' != "' . $value . '"' . &ignore_case( $key, $case_sensitivity ) . ')';
    }
    else {
        return '(' . $key . ' = "' . $value . '"' . &ignore_case( $key, $case_sensitivity ) . ')';
    }
}

sub intersection {
    my ( $array1, $array2 ) = @_;
    my %count = ();
    my @intersection;
    foreach my $element ( @$array1, @$array2 ) {
        $count{$element}++;
    }
    foreach my $element ( keys %count ) {
        push( @intersection, $element ) if ( $count{$element} > 1 );
    }
    return \@intersection;
}

sub ignore_case {
    return ( ( $_[0] eq "word" or $_[0] eq "lemma" ) and not $_[1] ) ? ' %c' : '';
}

sub match {
    my ( $depth, $query, $p_attributes, $depth_to_query_node_ref, $candidates ) = @_;
    my $query_node = $depth_to_query_node_ref->{$depth};
    my $result;

    # if matching has been successful, return matching corpus
    # positions
    if ( $depth > $#$query ) {
        @$candidates = map { $_->[0] } @$candidates;
        return [$candidates];
    }

    # collect incoming dependency relations for query_node in query graph
    my ( $number_of_incoming_rels, @query_incoming_rels );
    foreach my $i ( 0 .. $#$query ) {
        if ( $query->[$i]->[$query_node] ) {
            $query_incoming_rels[$i] = $query->[$i]->[$query_node]->{"relation"};
        }
    }
    $number_of_incoming_rels = grep {defined} @query_incoming_rels;

    # collect outgoing dependency relations for query_node in query graph
    my ( $number_of_outgoing_rels, @query_outgoing_rels );
    foreach my $i ( 0 .. $#$query ) {
        if ( $query->[$query_node]->[$i] ) {
            $query_outgoing_rels[$i] = $query->[$query_node]->[$i]->{"relation"};
        }
    }
    $number_of_outgoing_rels = grep {defined} @query_outgoing_rels;

CPOS: foreach my $cpos ( @{ $candidates->[$query_node] } ) {

        # store corpus position for current query_node, remove it from
        # other nodes
        my $local_candidates;
        for my $query_node ( 0 .. $#$candidates ) {
            $local_candidates->[$query_node] = [ grep { $_ != $cpos } @{ $candidates->[$query_node] } ];
        }
        $local_candidates->[$query_node] = [$cpos];

        # collect incoming dependency relations for corpus position
        my ( $indeps, @indeps );
        $indeps = $p_attributes->{"indep"}->cpos2str($cpos);
        $indeps =~ s/^\|//;
        $indeps =~ s/\|$//;
        @indeps = split( /\|/, $indeps );
        next CPOS if ( @indeps < $number_of_incoming_rels );

        # collect outgoing dependency relations for corpus position
        my ( $outdeps, @outdeps );
        $outdeps = $p_attributes->{"outdep"}->cpos2str($cpos);
        $outdeps =~ s/^\|//;
        $outdeps =~ s/\|$//;
        @outdeps = split( /\|/, $outdeps );
        next CPOS if ( @outdeps < $number_of_outgoing_rels );

        # collect corpus position of the start nodes of the incoming
        # dependency relations
        my @corpus_candidates;
        for ( my $i = 0; $i <= $#query_incoming_rels; $i++ ) {
            my $rel = $query_incoming_rels[$i];
            next unless ( defined($rel) );
            foreach my $indep (@indeps) {
                if ( $indep =~ m/^(?<relation>$rel)\((?<offset>-?\d+)(?:&apos;)*,/ ) {
                    my $offset     = $+{"offset"};
                    my $start_cpos = $cpos + $offset;
                    push( @{ $corpus_candidates[$i] }, $start_cpos );
                }
            }
            @{ $corpus_candidates[$i] } = uniq @{ $corpus_candidates[$i] };
        }

        # collect corpus position of the start nodes of the outgoing
        # dependency relations
        for ( my $i = 0; $i <= $#query_outgoing_rels; $i++ ) {
            my $rel = $query_outgoing_rels[$i];
            next unless ( defined($rel) );
            foreach my $outdep (@outdeps) {
                if ( $outdep =~ m/^(?<relation>$rel)\(0(?:&apos;)*,(?<offset>-?\d+)(?:&apos;)*\)$/ ) {
                    my $offset     = $+{"offset"};
                    my $start_cpos = $cpos + $offset;
                    push( @{ $corpus_candidates[$i] }, $start_cpos );
                }
            }
            @{ $corpus_candidates[$i] } = uniq @{ $corpus_candidates[$i] };
        }

        # intersect candidate corpus positions for connected nodes
        # with those corpus positions that are actually connected to
        # the current node
        for ( my $i = 0; $i <= $#$local_candidates; $i++ ) {
            next if ( !defined $corpus_candidates[$i] );
            $local_candidates->[$i] = &intersection( $local_candidates->[$i], $corpus_candidates[$i] );
            next CPOS if ( @{ $local_candidates->[$i] } == 0 );
        }

        # recursion
        my $local_result = &match( $depth + 1, $query, $p_attributes, $depth_to_query_node_ref, $local_candidates );
        if ( defined $local_result ) {
            push @{$result}, @$local_result;
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
