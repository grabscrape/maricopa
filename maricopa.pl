#!/usr/bin/perl

use strict;
use Pod::Usage;
use Getopt::Long;
use IO::Handle;
STDERR->autoflush(1);
#STDOUT->autoflush(1);

require MaricopaData;

our $_debug;
our $_dumper;
our $_scrape_dump;
our $_verbose;
BEGIN {
  $_debug=0;
  $_verbose=2;
  $_scrape_dump=0;
  $_dumper=1;
  eval 'use Data::Dumper' if $_dumper;
}


our  $_log_string;
our  $_version =        2; 
our  $_type    = 'subdivision'; # or 'street'
my  ($_help,$_man);


GetOptions('type=s'    =>\$_type,
           'version=i'=>\$_version,
           'verbose=i'=>\$_verbose,
           'help|?'   =>\$_help,
           'man'      =>\$_man) or pod2usage(2);

pod2usage(1) if $_help;
pod2usage(-exitstatus => 0, -verbose => 2) if $_man;

die "Incorrect value for '-verbose' option." 
       unless( $_verbose == 1 or $_verbose == 2 );
die "Incorrect value for '-type' option." 
       unless( $_type eq 'street' or $_type eq 'subdivision' );
  

&MaricopaData::db_prepare;


## Main loop (recursive if needed) here;
&GoWWW::tryToScrapData(
      &GoWWW::uri,
      GoWWW::queryParams(),
      GoWWW::significantParam(), '%s', $_ ) for @{ &::mode_rank };

&::_log_reset;

### Body end;


sub mode_rank {
  # we determine - first or rest call
  # the char sequence is different;
  my $mode = 'rest';
  $mode = 'first' if (caller)[0] eq 'main';

  my %values= (
    subdivision=> {
      first=> [ ('a'..'z') ],
      rest => [ (' ','&',',','-','.','/','0'..'9','@','a'..'z') ] }
   ,street     => {
      first=> [ ('0'..'9') ],
      rest => [ ('0'..'9') ] } );

  return $values{$::_type}{$mode};
}

###
# loging
our $_log_last_length;
sub _log_out {
  my $str = join '', @_;
  $_log_last_length += length $str;
  print STDERR $str;
}
sub _log_add {
  my $str = join '', @_;
  print STDERR $str;
}

sub _log_reset {
  $_log_last_length=0;
  print STDERR "\n";
}
sub _log_newline {
  print STDERR "\n".('.'x$_log_last_length);
}

## srand/rand section
my $srand_executed;
sub random_delay {
  srand time unless $srand_executed;
  return int rand( 12 );
}
#
#
#### End Main Package

package GoWWW;
use strict;
use LWP::UserAgent;

my $_globalData;
our $root_uri;
BEGIN {
eval 'use Data::Dumper' if $::_dumper;
$root_uri='http://www.maricopa.gov/';
}

sub queryParams {
  my $toRestore =$_; #
  my %_queryParams = (
    'subdivision'=> { 'UcAdvancedSearch1:btnSubmitSub.x'=>0
                     ,'UcAdvancedSearch1:btnSubmitSub.y'=>0
                     , %{ &GoWWW::paramScope } }
   ,'street'     => { 'UcAdvancedSearch1:btnSubmitStreet.x'=>0
                     ,'UcAdvancedSearch1:btnSubmitStreet.y'=>0
                     , %{ &GoWWW::paramScope } }
  );
  $_ = $toRestore;
  return $_queryParams{$::_type};
}

sub significantParam {
  my %_significantParam = (
    'subdivision' => 'UcAdvancedSearch1:subf',
    'street'      => 'UcAdvancedSearch1:numberf'
  );
  return $_significantParam{$::_type};
}

sub paramScope {
  
  my $start_uri=
    $root_uri.'Assessor/ParcelApplication/Default.aspx';

  my %predefined_params;
  #my $_mech_dump = 'mech-dump.bat'; # for cygwin
  my $_mech_dump = 'mech-dump'; # libwww-mechanize-perl package in Ubuntu.

  open MD, $_mech_dump." -forms ".$start_uri.' |' or die "Can't find ", $_mech_dump, ": $!\n";
  while( <MD> ) {
    chop;
    # some operations to hande mech-dump output
    s/^\s*//; # cut leading spaces
    s/\s{2}.+$//;
    next if m/(^POST\s|\(image\)$|^$)/; #cut POST line + submit (image) lines
    s/\s+\(.+\)$//; # cut trailing round brackets (hidden for example)

    $predefined_params{$1} =$2 if m/(\S+?)=(.*)$/;
  }
  close MD;

  return \%predefined_params;
}

sub getPage {
  my $uri       = shift;
  my $param     = shift;
  my $discrete  = shift;
  my $ptr; # ptr to returned data;
  my $tmp;
  my ($type,$data_ptr);
  

RESTART:
  undef $type;
  if( $::_debug ) {
    my $debug_data_file = 'street.html';
    #$debug_data_file    = 'subdivision2.html' if $::_type eq 'subdivision';
    $debug_data_file    = 'aa.html' if $::_type eq 'subdivision';
    open GAG, $debug_data_file or die "Debug data file ($debug_data_file) failure: $!";
    $tmp .= $_ for <GAG>;
    close GAG;
    $ptr= \$tmp;
  } else {
    my $ua = LWP::UserAgent->new;
    #$ua->env_proxy;
    my $response;
    if( $param ) {
      $response=$ua->post( $uri, [ $param ] );
    } else {
      $response = $ua->get( $uri );
    }
    unless( $response->is_success ) {
      ($type,$data_ptr) = &handleAbnormal( $response, $discrete );
      if( $type =~ /^parcel/ ) {
        $ptr = $data_ptr;
      } elsif( $type eq 'currently unavailable' ) {
        my $delay = &::random_delay;
        &::_log_add("curr unavail[$delay],");
        sleep( $delay );
        goto RESTART;
      } elsif( $type eq 'reset by peer' ) {
        my $delay = &::random_delay;
        &::_log_add("reset by peer[$delay]," );
        sleep( $delay );
        goto RESTART;
      } elsif( $type eq 'service unavailable' ) {
        my $delay = &::random_delay;
        &::_log_add("service unavailable[$delay]," );
        sleep( $delay );
        goto RESTART;
      } else {
        print STDERR "Unhundled situation: ", 
               $response->status_line, "\n", $response->as_string, "\n"; 
        exit;
      }
    } else {

      $ptr = \$response->decoded_content;
    }
  }
  return ($type, $ptr);
}

my @_stackStatuses;
sub parseData {
  my $type     = shift;
  my $data_ptr = shift;

  my ($traectory, $number, $msg);

  return 'too_long' if $$data_ptr =~ m/Your search criteria returned 1000\+/;

  unless( $type ) {
    if ( $$data_ptr =~ m/Your search for (Subdivision|MCR|Street Address):.+?\W(\d*)\W+(\S+?)\./ ) {
      $number = $2;
      $traectory =$3;
      $msg = $3;
      $msg = 'subdiv\'s' if $traectory eq 'subdivisions';
      $msg = "$msg list/$number:";
      &::_log_newline if $msg !~ /sub/ and $_verbose ==2;
      &::_log_out ( "$msg" );
    } elsif ( 
       $$data_ptr =~ m/Assessor - Residential Parcel Information/ and
       $$data_ptr =~ m#<span id="_ctl0_lblParcel">(.+?)</span># ) {

       $data_ptr = getParcel( $1 );
       $msg = "redirect to parcel '$1'";
       &::_log_add ( " $msg" ) if $::_verbose==2;
       $traectory = "parcel";
    }
  } else {
    $traectory =$type;
    
  }

  push @_stackStatuses, $traectory;
  my $cnt=0;
  if( $traectory ne "parcel" ) {

    foreach my $line (split /\n/, $$data_ptr ) {
      chop $line;
      if( $traectory eq 'subdivisions' || $traectory eq 'MCRs') {

        if( $line =~ m#<td><a href="(/Assessor/ParcelApplication/Default.aspx\?Subdiv=.+?)">(.+?)</a></td><td><a href="(/Assessor/ParcelApplication/Default.aspx\?MCR=.+?)">(.+?)</a></td><td>(.+?)</td># ) {


          my $pre_length= $::_log_last_length;
          if( $::_verbose ) {
            &::_log_newline;
            &::_log_out( "MCR: '$4'" );
            &::_log_add( ": " )       if $::_verbose ==1;
          }
          # Error when MCR: '4410 ' and parcel: '166-21-010 ' 
          #            MCR: '65223' and         '303-43-685 '
          #            MCR: '118'
          # fixed.


          # args: 1 - MCR number $4
          #       2 - Subdivison name $2
          #       3 - city: $5

          $_globalData->{mcr} = $4;
          my $msg= &MaricopaData::store_assessor_subdivision( $4, $2, $5, 0 );
          &::_log_add( $msg );

          
          my( $t,$d);

          if( $_stackStatuses[1] eq 'MCRs'
                  and $_stackStatuses[2] eq 'subdivisions' ) {
            $::_log_last_length = $pre_length;
            &::_log_add( 'cycle broke' );
            next;
          } 
     

#          ($t,$d) = getPage( $root_uri.$3, undef, undef );
          if(      $_stackStatuses[0] eq 'subdivisions'
              and  $_stackStatuses[1] eq 'MCRs' ) {
            ($t,$d) = getPage( $root_uri.$1, undef, undef );
          } else {
            ($t,$d) = getPage( $root_uri.$3, undef, undef );
          }

          my ($rv,$hash_data ) = &parseData(undef,$d);#  ; #, Dumper $d;

          $::_log_last_length = $pre_length;

          $cnt++;
        }
      } elsif( $traectory eq "parcels" ) {
        if(  $line =~ m#.*<td><a href="/Assessor/ParcelApplication/Detail.aspx\?ID=.+?">(.+?)\s*</a></td><td>(.+?)</td><td>(.+?)</td><td>(.+?)</td># ) {

          if( $::_verbose ) {
            &::_log_newline             if $::_verbose==2;
            &::_log_add( " '$1'" ) if $::_verbose ==2;
          }

          $cnt++;

          if( &MaricopaData::is_parcel_exists( $1 ) ) {
            &::_log_add( ' skip' ) if $::_verbose ==2;
            &::_log_add( 's' )     if $::_verbose ==1;
            
            next;
          }

          # Error when MCR: '4410 ' and parcel: '166-21-010 ' 
          #            MCR: '65223' and         '303-43-685 '
          #next if $1 ne '303-43-685 ';
          $_globalData->{parcel} = $1;

          
          my ($t,$d)= getPage( $root_uri.
                    "/Assessor/ParcelApplication/DetailPrinterFriendly.aspx?ID=$1",
                           undef, undef ); # 
          &parseData( 'parcel', $d );
          
        }
      }
    }
  } elsif( $traectory eq 'parcel' ) {

    my $h;
    if( $$data_ptr =~ m/The page cannot be found\./ ) {

      $msg= ' inactive' if $::_verbose==2;
      $msg= 'i'         if $::_verbose==1;
      &::_log_add($msg);
      &MaricopaData::store_assessor_scrape( 
           { INACTIVE=>1, mcr=>$_globalData->{mcr},
              parcel=>$_globalData->{parcel} } );
      $cnt =1;
    } else {
      $h =  parseDetail( $data_ptr );
      # print Dumper $h if $::_scrape_dump;

      # restoring MCR record.
      if( $h->{info}->{mcr} && $::_type eq 'street' ) {
        &MaricopaData::store_assessor_subdivision( $h->{info}->{mcr}, 
           $h->{info}->{subdivision_name}, undef, 1 );
      }

      my $msg = &MaricopaData::store_assessor_scrape( $h->{info} );

      map { $_->{parcel} = $h->{info}->{parcel} } @{ $h->{valuation}};

      &MaricopaData::store_assessor_valuation( $h->{valuation} )
           unless $msg =~ /^\s*(failed|skip)/i;

      &::_log_add($msg); # if $::_verbose==2;
      $cnt =1;
    }
  }
  pop @_stackStatuses;
  return $cnt;
}


sub tryToScrapData {
  my $uri    = shift;
  my $params = shift;
  my $key    = shift;
  my $pre    = shift;
  my $val    = shift;
  my $result = sprintf "$pre", $val;
  
  # add finalliy '%' sign ( for SQL LIKE statemtnt on server side )
  # for example 'a ' query ( please, test on Firefox; my IE did not understand
  # differences;
  $params->{$key} = $result.'%';

  &::_log_reset;  
  &::_log_out( sprintf '%-5s', $result.'%' );

  my( $type, $data ) = getPage( $uri, $params, $result );

  my $rv = &parseData($type,$data);


  if( $rv eq 'too_long' ) {
    &::_log_add( 'failed (too long)' );
    sleep(1);
    &tryToScrapData( $uri, $params, $key, $result.'%s', $_ )
                                             for @{ &::mode_rank };
  } elsif( $rv ) {
    #&::_log_add( "$rv records scraped") if $::_verbose ==1;
    return 1;
  } else {
    &::_log_add( "empty");
    return 1;
  }



}

sub uri {
  my %_uri = (
     subdivision=>$root_uri.'Assessor/ParcelApplication/Default.aspx'
    ,street     =>$root_uri.'Assessor/ParcelApplication/Default.aspx'
  );

  return $_uri{$::_type};
}


sub getParcel {
  my $parcel = shift;
  my $detail="${root_uri}Assessor/ParcelApplication/DetailPrinterFriendly.aspx?ID=$parcel";

  my $ptr;
  my $tmp;
  if( $::_debug ) {
    my $debug_data_file = 'detail.html';
    open GAG, $debug_data_file or die "Debug data file failure: $!";
    $tmp .= $_ for <GAG>;
    close GAG;
    $ptr= \$tmp;

  } else {
    my $ua = LWP::UserAgent->new;
    my $response = $ua->get( $detail );
    unless( $response->is_success ) {
      print STDERR "\nDetail", $response->status_line, "\n",
                   $response->as_string;
      exit;
    }
    $ptr = \$response->decoded_content;
  }
  return   $ptr;
}

use HTML::TableParser;
sub parseDetail {
  my $detail_page = shift;
  my %tables_shift = (
     info           =>{ id=>'1.1.2', res=>'info',      f=>\&info_f }
    ,owner          =>{ id=>'1.1.4', res=>'info',      f=>\&info_f }
    ,valuation      =>{ id=>'1.1.6', res=>'valuation', f=>\&valuation_f }
    ,characteristics=>{ id=>'1.1.8', res=>'info',      f=>\&char_f }
  );

  my @cumulative_data;
  my $row = sub { my ($id,$line,$cols,$udata)=@_;
              push @cumulative_data, $cols };

  my (@reqs,$p);
  my $return_hash={};
  for my $i ( qw/info owner valuation characteristics/ ) {

    next unless $tables_shift{$i}->{res};
    @cumulative_data=();
    @reqs = ( { id=>$tables_shift{$i}->{id},
                row=>\&$row } );

    $p = HTML::TableParser->new( \@reqs
              ,{ Decode => 1, Trim => 1, Chomp => 1, DecodeNBSP=>1 } );
    $p->parse( $$detail_page );

    my $a=$tables_shift{$i}->{f};

    my $data = &$a( \@cumulative_data );

    if( (my $t=ref $data) eq 'HASH' ) {
      map { $return_hash->{$tables_shift{$i}{res}}->{$_} = $data->{$_} }  keys %{ $data };
    } elsif( $t eq 'ARRAY' ) {
      $return_hash->{$tables_shift{$i}{res}} = $data;
    }
  } 
  return $return_hash;
}

sub keys_normalize_info {
  my $k = shift;

  $k =~ s/(#|:)//g;
  $k =~ s/\s+$//;
  $k =~ s/^\s+//;
  $k =~ s/(\s|\/)/_/g;


  # for MCR: '65223' and         '303-43-685 '
  # '*property_previously_owned_by_a_bank,__mortgage_company_or_other_financial_institution' =>
  #      '*Property previously owned by a bank,  mortgage company or other financial institution'
  $k =~ s/^\*(Property_previously).*/$1/;
  return lc $k;
}

sub keys_normalize_valuation {
  my $k = shift;

  $k =~ s/(#|:)//g;
  $k =~ s/\s+$//;
  $k =~ s/^\s+//;
  $k =~ s/(\s|\/)/_/g;
  $k =~ s/(\(|\))//g;

  return lc $k;
}

sub keys_normalize_ch {
  my $k = shift;
  my $char_prefix='ch_';

  $k =~ s/(#|:)//g;
  $k =~ s/\s+$//;
  $k =~ s/^\s+//;
  $k =~ s/(\s|\/)/_/g;
  $k =~ s/(\(|\))//g;

  return $char_prefix.( lc $k);
}

sub char_f {
  my $p = shift;
  my %ret;
  foreach my $c ( @{ $p } ) {
    $ret{keys_normalize_ch($c->[1])} = $c->[2] if $c->[2];
    $ret{keys_normalize_ch($c->[4])} = $c->[5] if $c->[5];
  }
  return \%ret;
}

sub info_f {
  my $p = shift;
  my %ret =();

  %ret = map { keys_normalize_info($_->[0])=>$_->[1] } grep $_->[0] ne '' || $_->[1] ne '' ,@{ $p };

  return \%ret;
}

sub valuation_f {
  my $p = shift;
  my @ret; # array for returned data  = [{},{},{}];
  my @traectory;
  my $field;
  my ($c,$c1)=0;
  foreach my $tag ( @{ $p->[0] } ) {
    $c++;
    next unless $tag;
    $c1++;
    push @ret, {} if $c1 != 1; #1st element is field name;
    push @traectory, $c-1;
  }

  my $s;
  foreach my $tag ( @{ $p } ) {
   $s=0; $s += length $_ for @{ $tag };
   next unless $s;
  
   $field = keys_normalize_valuation( $tag->[ $traectory[0] ] );
   next if $field =~ m/^notice_the_values/;
   my $c=0;
   foreach my $data ( @traectory[1..$#traectory] ) {
     $ret[$c++]->{$field}=$tag->[$data]; 
   }
  }
  return \@ret;
}


sub handleAbnormal {
  my $response =shift;
  my $data     =shift;
  if( $response->status_line =~ m/^302\D/ ) {
    if( $response->as_string =~ m{\nLocation:\s*/Assessor/ParcelApplication/Detail\.aspx\?ID=(.+?)\n}s ) {
      # just redirect to detail page
      # &::_log_out( "Redirect to Detail $1");
      my $ret = getParcel( $1 );  
      return "parcel $1", $ret;
    } elsif ( $response->as_string =~ m{\nLocation:\s*/Assessor/Error\.aspx\?type=database\&data=(Subdivision|Street Address):(.+?)\n}s ) {
      return 'currently unavailable', undef;
    }
  } elsif( $response->status_line =~ m/500\D/ ) {
    return 'reset by peer';
  } elsif( $response->status_line =~ m/503\D/ ) {
    return 'service unavailable';
  }
}

exit;

__END__

=head1 NAME

ms.pl - the scrapper for 'maricopa.gov' 

=head1 SYNOPSIS

ms.pl [options]

 Options:
   -type          street|subdivision
   -version       <integer value>. optional. prefix for store
                  data to database
   -help          brief help message
   -man           full documentation

=head1 OPTIONS

=over 4

=item B<-verbose 1|2>

Verbose staff output (stderr). 1 - several parcels on one line;
2 - one parcel detail per one line (default)

Log legenda for parcels: ( for -verbose 1 mode )
s - skip (already saved)
i - parcel is 'inactive'
+ - parcel saved

=item B<-help>

Print a brief help message and exits.

=item B<-man>

Prints the manual  page and exits.

=back

=head1 DESCRIPTION

 This script perform search according to the type:
 -type <street|subdivision> and store data to database

=cut

