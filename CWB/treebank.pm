package CWB::treebank;

use warnings;
use strict;

use Storable qw( dclone );
use JSON;
use Data::Dumper;

###TODO
my $bnc_tokens = 112254391;
###/TODO

sub match_graph {
    my ( $output, $cqp, $corpus_handle, $corpus, $querymode, $queryref, $case_sensitivity, $cache_handle ) = @_;
    local $Data::Dumper::Indent = 0;
    local $Data::Dumper::Purity = 1;
    my $query = decode_json($queryref);
    my @result;
    $cqp->exec($corpus);
    my $sentence = $corpus_handle->attribute( "s",    "s" );
    my $s_id     = $corpus_handle->attribute( "s_id", "s" );
    my %p_attributes;
    $p_attributes{"word"}   = $corpus_handle->attribute( "word",   "p" );
    $p_attributes{"pos"}    = $corpus_handle->attribute( "pos",    "p" );
    $p_attributes{"lemma"}  = $corpus_handle->attribute( "lemma",  "p" );
    $p_attributes{"wc"}     = $corpus_handle->attribute( "wc",     "p" );
    $p_attributes{"indep"}  = $corpus_handle->attribute( "indep",  "p" );
    $p_attributes{"outdep"} = $corpus_handle->attribute( "outdep", "p" );

    # cancel if there are no restrictions
    my $local_queryref = $queryref;
    $local_queryref =~ s/"(not_)?(relation|word|pos|lemma|wc|indep|outdep)"://g;
    $local_queryref =~ s/"\.[*+?]"//g;
    if ( $local_queryref =~ m/^[][}{, ]*$/ ) {
        print $cache_handle Data::Dumper->Dump( [ [ "", {} ] ], ["dump"] ), "\n";
        return;
    }

    # check frequencies
    my %frequencies;
    &check_frequencies( $cqp, $query, \%frequencies, $case_sensitivity );

    # execute query
    my @ids;
    my @freq_alignment;
    my @inverse_alignment;
    &execute_query( $cqp, $s_id, $query, \@ids, \@freq_alignment, \@inverse_alignment, $case_sensitivity, %frequencies );

    # match
    foreach my $sid ( keys %{ $ids[$#ids] } ) {
        my @candidates;
        my %used_up_positions;
        foreach my $token ( 0 .. $#$query ) {
            $candidates[$token] = [ @{ $ids[ $freq_alignment[$token] ]->{$sid} } ];
        }
        my $token = 0;
        my $result = &match_recursive( $query, \%p_attributes, \@freq_alignment, $token, \@candidates, $sid, \%used_up_positions );
        if ( ref($result) eq "HASH" ) {
            print $cache_handle Data::Dumper->Dump( [ [ $sid, $result ] ], ["dump"] ) . "\n";
        }
    }
}

sub transform_output {
    my ( $corpus_handle, $querymode, $sid, $result ) = @_;
    if ( $sid eq "" and %$result == 0 ) {
        return encode_json( {} );
    }
    my $s_id = $corpus_handle->attribute( "s_id", "s" );
    my %p_attributes;
    $p_attributes{"word"}   = $corpus_handle->attribute( "word",   "p" );
    $p_attributes{"pos"}    = $corpus_handle->attribute( "pos",    "p" );
    $p_attributes{"lemma"}  = $corpus_handle->attribute( "lemma",  "p" );
    $p_attributes{"wc"}     = $corpus_handle->attribute( "wc",     "p" );
    $p_attributes{"indep"}  = $corpus_handle->attribute( "indep",  "p" );
    $p_attributes{"outdep"} = $corpus_handle->attribute( "outdep", "p" );
    my ( $start, $end ) = $s_id->struc2cpos($sid);

    if ( $querymode eq "collo-word" ) {
        return encode_json( { "s_id" => $s_id->struc2str($sid), "tokens" => [ &to_string( \%p_attributes, $result, "word" ) ] } );
    }
    elsif ( $querymode eq "collo-lemma" ) {
        return encode_json( { "s_id" => $s_id->struc2str($sid), "tokens" => [ &to_string( \%p_attributes, $result, "lemma" ) ] } );
    }
    elsif ( $querymode eq "sentence" ) {
        return encode_json( { "s_id" => $s_id->struc2str($sid), "sentence" => [ $p_attributes{"word"}->cpos2str( $start .. $end ) ], "tokens" => [ &to_relative_position( $result, $start ) ] } );
    }
}

sub check_frequencies {
    my ( $cqp, $query, $frequencies, $case_sensitivity ) = @_;
    for ( my $i = 0; $i <= $#$query; $i++ ) {
        my $token = $query->[$i]->[$i];

        # Note: Try out adding indeps and outdeps information
        my $querystring = join( " & ", map( &map_token_restrictions( $_, $token->{$_}, $case_sensitivity ), keys %$token ) );

        #my $querystring = &build_query($query, $i, $case_sensitivity); # inefficient
        if ($querystring) {
            $cqp->exec("A = [$querystring]");
            ( $frequencies->{$i} ) = $cqp->exec("size A");
        }
        else {
            $frequencies->{$i} = $bnc_tokens;

            #die("It seems that the input graph is not connected. Is that right?\n");
        }
    }
}

sub execute_query {
    my ( $cqp, $s_id, $query, $ids, $freq_alignment, $inverse_alignment, $case_sensitivity, %frequencies ) = @_;
    foreach my $i ( sort { $frequencies{$a} <=> $frequencies{$b} } keys %frequencies ) {
        push( @$ids, {} );
        $freq_alignment->[$i] = $#$ids;
        push( @$inverse_alignment, $i );
        my $querystring = &build_query( $query, $i, $case_sensitivity );

        #print $querystring, "\n";
        $cqp->exec("[$querystring]");
        if ( ( $cqp->exec("size Last") )[0] > 0 ) {
            foreach my $match ( $cqp->exec("tabulate Last match") ) {
                my $sid = $s_id->cpos2struc($match);
                push( @{ $ids->[$#$ids]->{$sid} }, $match );
            }
        }

        #print scalar(keys %{$ids->[$#$ids]}), "\n";
        $cqp->exec("Last expand to s");
        $cqp->exec("Last");
    }
}

sub build_query {
    my ( $query, $i, $case_sensitivity ) = @_;
    my $token = $query->[$i]->[$i];
    my @querystring;

    #my $tokrestr = join( " & ", map( '(' . $_ . ' = "' . $token->{$_} . '"' . &ignore_case($_) . ")", keys %$token ) );
    my $tokrestr = join( " & ", map( &map_token_restrictions( $_, $token->{$_}, $case_sensitivity ), keys %$token ) );
    push( @querystring, $tokrestr ) if ($tokrestr);
    my $indeps = join( ' & ', map( '(indep contains "' . $_ . '\(.*")', grep( defined, map( $query->[$_]->[$i]->{"relation"}, ( 0 .. $#$query ) ) ) ) );
    $indeps .= ' & (ambiguity(indep) >= ' . scalar( grep( defined, map( $query->[$_]->[$i]->{"relation"}, ( 0 .. $#$query ) ) ) ) . ')' if ($indeps);
    push( @querystring, $indeps ) if ($indeps);
    my $outdeps = join( ' & ', map( '(outdep contains "' . $_ . '\(.*")', grep( defined, map( $query->[$i]->[$_]->{"relation"}, ( 0 .. $#$query ) ) ) ) );
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

sub match {
    my ( $query, $p_attributes, $freq_alignment, $token, $cpos, $candidates, $sid, $used_up_positions ) = @_;
    my @filter;
    my %result;
    my $indeps = $p_attributes->{"indep"}->cpos2str($cpos);
    $indeps =~ s/^\|//;
    $indeps =~ s/\|$//;
    my @indeps = split( /\|/, $indeps );
    my @rels;

    foreach my $i ( 0 .. $#$query ) {
        if ( $query->[$i]->[$token] ) {
            $rels[$i] = $query->[$i]->[$token]->{"relation"};
        }
    }
REL: for ( my $i = 0; $i <= $#rels; $i++ ) {
        my $rel = $rels[$i];
        next REL unless ( defined($rel) );
        my $found = 0;
        foreach my $indep (@indeps) {
            if ( $indep =~ m/^$rel\((?<offset>-?\d+)('*),/ ) {
                my $offset = $+{"offset"};
                $offset = "+" . $offset unless ( substr( $offset, 0, 1 ) eq "-" );
                push( @{ $filter[$i] }, eval "$cpos$offset" );
                $found++;
            }
        }
        return unless ($found);

        # do a "uniq"
        my %seen;
        @{ $filter[$i] } = grep { !$seen{$_}++ } @{ $filter[$i] };
    }
    for ( my $i = 0; $i <= $#$candidates; $i++ ) {
        next unless ( $filter[$i] );
        $candidates->[$i] = &intersection( $candidates->[$i], $filter[$i] );
        return unless ( @{ $candidates->[$i] } );
    }
    if ( $token < $#$query ) {
        $token++;
        return &match_recursive( $query, $p_attributes, $freq_alignment, $token, $candidates, $sid, $used_up_positions );
    }
    else {
        return 1;
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

sub remove_used_up_positions {
    my ( $local_candidates, $used_up_positions ) = @_;
    for ( my $i = 0; $i <= $#$local_candidates; $i++ ) {
        my $foo = $local_candidates->[$i];
        for ( my $j = 0; $j <= $#$foo; $j++ ) {

            # remove corpus position at $local_candidates->[$i]->[$j]
            # if the corpus position is in $used_up_positions and the
            # node stored there is not the one represented by $i;
            # i.e. corpus positions already in use cannot be used for
            # another node
            splice( @{ $local_candidates->[$i] }, $j, 1 ) if ( defined( $used_up_positions->{ $local_candidates->[$i]->[$j] } ) and $i != $used_up_positions->{ $local_candidates->[$i]->[$j] } );
        }
    }
}

sub to_string {
    my ( $p_attributes, $result, $attribute ) = @_;
    my @collection;
    foreach my $query_number ( keys %$result ) {
        foreach my $cpos ( keys %{ $result->{$query_number} } ) {
            my $word = $p_attributes->{$attribute}->cpos2str($cpos);
            if ( $result->{$query_number}->{$cpos} == 1 ) {

                #push( @collection, "$query_number $word" );
                push( @collection, [$word] );
            }
            else {
                foreach my $deeper ( &to_string( $p_attributes, $result->{$query_number}->{$cpos}, $attribute ) ) {

                    #push( @collection, "$query_number $word $deeper" );
                    push( @collection, [ $word, @$deeper ] );
                }
            }
        }
    }
    return @collection;
}

sub to_relative_position {
    my ( $result, $start ) = @_;
    my @collection;
    foreach my $query_number ( keys %$result ) {
        foreach my $cpos ( keys %{ $result->{$query_number} } ) {
            if ( $result->{$query_number}->{$cpos} == 1 ) {
                push( @collection, [ $cpos - $start ] );
            }
            else {
                foreach my $deeper ( &to_relative_position( $result->{$query_number}->{$cpos}, $start ) ) {
                    push( @collection, [ $cpos - $start, @$deeper ] );
                }
            }
        }
    }
    return @collection;
}

sub ignore_case {
    return ( ( $_[0] eq "word" or $_[0] eq "lemma" ) and not $_[1] ) ? ' %c' : '';
}

sub match_recursive {
    my ( $query, $p_attributes, $freq_alignment, $token, $candidates, $sid, $used_up_positions ) = @_;
    my %result;
    foreach my $cpos ( @{ $candidates->[$token] } ) {
        my $local_candidates        = dclone($candidates);
        my $local_used_up_positions = dclone($used_up_positions);
        $local_candidates->[$token] = [$cpos];
        $local_used_up_positions->{$cpos} = $token;
        &remove_used_up_positions( $local_candidates, $local_used_up_positions );
        my $rvalue = &match( $query, $p_attributes, $freq_alignment, $token, $cpos, $local_candidates, $sid, $local_used_up_positions );
        if ($rvalue) {
            $result{$token}->{$cpos} = $rvalue;
        }
    }
    return %result ? \%result : undef;
}

1;
