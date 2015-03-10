package MaricopaData;
use strict;
eval 'use Data::Dumper' if $::_dumper;
use DBI qw(:sql_types);

my $dbname='maricopa2';
my $host='localhost';
my $user='maricopa';
my $password='mAricoP7a';


my $dbh; 
my $subdivisions_table_name;
my $scrapes_table_name;
my $valuations_table_name;
my $sth_pre_subdivision;
my $sth_pre_parcel_exists;
my @scrape_fields;
my @valuation_fields;

our $store_to_db_func;


sub db_prepare {
 
  $dbh = DBI->connect("DBI:mysql:$dbname:$host", $user, $password );

  #$dbh = Mysql->connect('localhost','maricopa','maricopa','mAricoP7a');
  $subdivisions_table_name = "assessor_subdivisions_$::_version";
  $scrapes_table_name      = "assessor_scrape_$::_version";
  $valuations_table_name   = "assessor_valuation_$::_version";

  my $sth;
  ##
  ## Subdivison table name
  $sth = $dbh->prepare( <<EOD
    CREATE TABLE IF NOT EXISTS $subdivisions_table_name (
      mcr           VARCHAR(1000) PRIMARY KEY, /* Not int. See "A.L. MOORE TRACT" */
      subdivision   TEXT(1024),
      city_name     TEXT(1024),
      restored_flag INTEGER /* '1' if record created based parcel data */
)
EOD
);
  $sth->execute;

  # it is make cense to create bind pre-query.
  $sth_pre_subdivision = $dbh->prepare(
    "INSERT INTO $subdivisions_table_name VALUES (?,?,?,?)");

  ##
  ## Scrape table name 
  $sth = $dbh->prepare( <<EOD
    CREATE TABLE IF NOT EXISTS $scrapes_table_name (
      parcel VARCHAR(1000) PRIMARY KEY,
      mcr    VARCHAR(1024) REFERENCES $subdivisions_table_name (mcr)
)
EOD
);
  $sth->execute;


  # it is useful to create bind pre-query
  $sth_pre_parcel_exists = $dbh->prepare( 
     "SELECT parcel from $scrapes_table_name WHERE parcel = ? " );

  @scrape_fields = getFields( $scrapes_table_name );

  ##
  ## Valuations table
  $sth = $dbh->prepare( <<EOD 
    CREATE TABLE IF NOT EXISTS $valuations_table_name ( 
      parcel VARCHAR(1000),
      INDEX (parcel)
)
EOD
);
  $sth->execute;

  # already existed fields
  @valuation_fields = getFields( $valuations_table_name );

}

sub getFields {
  my $t =shift;
  my $sth  = $dbh->prepare("show columns from $t");
  $sth->execute;

  my @a; # field names
  while (my $data = $sth->fetch ) {
    push @a, $data->[0];
  }
  return @a;
}

sub store_assessor_subdivision {
  my( $mcr_number, $subdivision_name, $city_name, $flag ) = @_;
  $sth_pre_subdivision->bind_param( 1, $mcr_number );
  $sth_pre_subdivision->bind_param( 2, $subdivision_name );
  $sth_pre_subdivision->bind_param( 3, $city_name );
  $sth_pre_subdivision->bind_param( 4, $flag ) or die "4";
 
  my $ret_status; 
  $ret_status = '';

  $sth_pre_subdivision->{PrintError} =0;
  unless ( $sth_pre_subdivision->execute ) {
    my $errstr = $sth_pre_subdivision->errstr;
    if( $errstr =~ /^Duplicate entry/ ) {
      $ret_status = ' Skip; ';
    } else {
      warn $errstr;
      $ret_status = ' ok';
    }
  }
  $sth_pre_subdivision->{PrintError} =1;
  return $ret_status;
}

sub addDetailFields {
  my $ef_p = shift; #existing fields ptr;
  my ($sth, $q);
  foreach my $f ( @{ $ef_p } ) {
    if( ! grep $_ eq $f, @scrape_fields ) {
      $q = "\nALTER TABLE $scrapes_table_name ADD COLUMN $f VARCHAR(1024)\n";
      $sth = $dbh->prepare( $q )  or die "\n\n$q\n\n";
      $sth->execute;
      push @scrape_fields, $f;
    }
  }
}

sub addValuationFields {
  my $ef_p = shift; #existing fields ptr;
  my ($sth, $q);
  foreach my $f ( @{ $ef_p } ) {
    if( ! grep $_ eq $f, @valuation_fields ) {
      $q = "ALTER TABLE $valuations_table_name ADD COLUMN $f TEXT";
      $sth = $dbh->prepare( $q )  or die "\n\n$q\n\n";
      $sth->execute;
      push @valuation_fields, $f;
    }
  }
}

sub store_assessor_scrape {
  my $p=shift;
  my @fields = keys %{ $p };

  addDetailFields( \@fields );

  my $cols_str  = join ',', @fields;
  my $vals_str .= join ',', map { $dbh->quote($p->{$_})} @fields;
  my $q = "INSERT INTO $scrapes_table_name ($cols_str) ".
          " VALUES ($vals_str)";

  my $sth = $dbh->prepare( $q ) or die $q;
  my $ret_status;
  if( $::_verbose == 2 ) {
    # we print the number of scraped fields ( info only; )
    $ret_status = ' ok ['. (scalar @fields) . ']';
  } else {
    $ret_status = '+';
  }

  $sth->{PrintError} = 0;
  unless( $sth->execute ) {
    my $errstr = $sth->errstr;
    if ( $errstr =~ /^Duplicate entry/i ) {
      $ret_status = ' Skip ' if $::_verbose ==2;
      $ret_status = 's' if $::_verbose ==1;
    } else {
      warn $errstr;
      $ret_status =' failed'
    }
  }
  $sth->{PrintError} = 1;
  return $ret_status;
}

sub store_assessor_valuation {
  my $ap = shift; # array ptr;

  my @fields;
  my ($sth,$q);
  foreach my $p ( @{ $ap } ) {
    @fields = keys %{ $p };
    addValuationFields( \@fields );

    $q = "INSERT INTO $valuations_table_name (".
     ( join ',',@fields ). ") VALUES (".
     (  join ',', map {$dbh->quote($p->{$_})} @fields). ')';

    $sth = $dbh->prepare($q) or die $q;
    $sth->execute;

  }
}

sub is_parcel_exists {
  my $pn = shift; # parcel number;

  $sth_pre_parcel_exists->bind_param ( 1,  $pn, {TYPE=>SQL_VARCHAR} );
  $sth_pre_parcel_exists->execute;

  if( $sth_pre_parcel_exists->fetch ) {
    return 1;
  }
  return 0;

}
1;

