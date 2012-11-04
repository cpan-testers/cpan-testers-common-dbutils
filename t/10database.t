#!/usr/bin/perl -w
use strict;

use Test::More;#  tests => 21;
use CPAN::Testers::Common::DBUtils;
use Data::Dumper;

eval "use Test::Database";
plan skip_all => "Test::Database required for DB testing" if($@);

plan 'no_plan';

#my @handles = Test::Database->handles();
#diag("handle: ".$_->dbd)    for(@handles);
#diag("drivers all: ".$_)    for(Test::Database->list_drivers('all'));
#diag("drivers ava: ".$_)    for(Test::Database->list_drivers('available'));

#diag("rcfile=".Test::Database->_rcfile());

# may expand DBs later
my $td;
if($td = Test::Database->handle( 'mysql' )) {
    create_mysql_databases($td);
} elsif($td = Test::Database->handle( 'SQLite' )) {
    create_sqlite_databases($td);
}

SKIP: {
    skip "No supported databases available", 21  unless($td);

#diag(Dumper($td->connection_info()));

    my %opts;
    ($opts{dsn}, $opts{dbuser}, $opts{dbpass}) =  $td->connection_info();
    ($opts{driver})    = $opts{dsn} =~ /dbi:([^;:]+)/;
    ($opts{database})  = $opts{dsn} =~ /database=([^;]+)/;
    ($opts{database})  = $opts{dsn} =~ /dbname=([^;]+)/     unless($opts{database});
    ($opts{dbhost})    = $opts{dsn} =~ /host=([^;]+)/;
    ($opts{dbport})    = $opts{dsn} =~ /port=([^;]+)/;
    my %options = map {my $v = $opts{$_}; defined($v) ? ($_ => $v) : () }
                        qw(driver database dbfile dbhost dbport dbuser dbpass);

#diag(Dumper(\%options));

    # create new instance from Test::Database object
    my $ct = CPAN::Testers::Common::DBUtils->new(%options);
    isa_ok($ct,'CPAN::Testers::Common::DBUtils');

    # test hash
    is( $ct->driver, $td->dbd, 'driver matches: ' . $ct->driver );

    # insert records
    my $sql = 'INSERT INTO cpanstats ( id, guid, state, postdate, tester, dist, version, platform, perl, osname, osvers, fulldate, type) VALUES ( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ? )';
    $ct->do_query( $sql,1396564,'1396564-ed372d00-b19f-3f77-b713-d32bba55d77f','unknown','201101','srezic@cpan.org','Acme-Buffy','1.5','i386-freebsd','5.5.5','freebsd','6.1-release','201101022114',2);
    $ct->do_query( $sql,1587804,'1587804-ed372d00-b19f-3f77-b713-d32bba55d77f','na','201101','jj@jonallen.info ("JJ")','AI-NeuralNet-SOM','0.07','darwin-2level','5.8.1','darwin','7.9.0','201101030648',2);
    $ct->do_query( $sql,1717321,'1717321-ed372d00-b19f-3f77-b713-d32bba55d77f','na','201101','srezic@cpan.org','Abstract-Meta-Class','0.10','i386-freebsd','5.5.5','freebsd','6.1-release','201101171653',2);
    $ct->do_query( $sql,1994346,'1994346-ed372d00-b19f-3f77-b713-d32bba55d77f','unknown','201101','srezic@cpan.org','AI-NeuralNet-SOM','0.02','i386-freebsd','5.6.2','freebsd','6.1-release','201101062212',2);
    $ct->do_query( $sql,2603754,'2603754-ed372d00-b19f-3f77-b713-d32bba55d77f','fail','201101','JOST@cpan.org ("Josts Smokehouse")','AI-NeuralNet-SOM','0.02','i86pc-solaris-64int','5.8.8 patch 34559','solaris','2.11','201101122105',2);
    $ct->do_query( $sql,2613077,'2613077-ed372d00-b19f-3f77-b713-d32bba55d77f','fail','201101','srezic@cpan.org','Acme-Buffy','1.5','i386-freebsd','5.8.9','freebsd','6.1-release-p23','201101132053',2);
    $ct->do_query( $sql,2725989,'2725989-ed372d00-b19f-3f77-b713-d32bba55d77f','pass','201101','stro@cpan.org','Acme-CPANAuthors-Canadian','0.0101','MSWin32-x86-multi-thread','5.10.0','MSWin32','5.00','201101011303',2);
    $ct->do_query( $sql,2959417,'2959417-ed372d00-b19f-3f77-b713-d32bba55d77f','pass','201101','rhaen@cpan.org (Ulrich Habel)','Abstract-Meta-Class','0.11','MSWin32-x86-multi-thread','5.10.0','MSWin32','5.1','201101301529',2);

    # select records
    my @arr = $ct->get_query('array','SELECT count(*) FROM cpanstats');
    is($arr[0]->[0], 8, '.. count all records');
    @arr = $ct->get_query('hash','SELECT count(*) AS count FROM cpanstats WHERE state=?','pass');
    is($arr[0]->{count}, 2, '.. count PASS records');

    @arr = $ct->get_query('array','SELECT * FROM cpanstats');
    is(@arr, 8, '.. retrieved all records');

    # interate over records
    my $next = $ct->iterator('hash','SELECT * FROM cpanstats');
    my $rows = 0;
    while(my $row = $next->()) {
        $rows++;
        is($row->{type},2,'.. matched type');
    }
    is($rows, 8, '.. iterated over all records');

    $next = $ct->iterator('array','SELECT * FROM cpanstats');
    $rows = 0;
    while(my $row = $next->()) {
        $rows++;
        is($row->[12],2,'.. matched type');
    }
    is($rows, 8, '.. iterated over all records');

    # test repeat queries & repeater   
    $sql = 'INSERT INTO cpanstats ( id, guid, state, postdate, tester, dist, version, platform, perl, osname, osvers, fulldate, type) VALUES ( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)';
    $ct->repeat_query( $sql, 
        2969661,'2969661-ed372d00-b19f-3f77-b713-d32bba55d77f','pass','201102','CPAN.DCOLLINS@comcast.net','Abstract-Meta-Class','0.11','i686-linux-thread-multi','5.10.0','linux','2.6.24-19-generic','201102010303',2
    );
    $ct->repeat_query( $sql, 
        2969663,'2969663-ed372d00-b19f-3f77-b713-d32bba55d77f','pass','201102','CPAN.DCOLLINS@comcast.net','Abstract-Meta-Class','0.11','i686-linux-thread-multi','5.10.0','linux','2.6.24-19-generic','201102010303',2
    );
    $ct->repeat_query( $sql, 
        2970367,'2970367-ed372d00-b19f-3f77-b713-d32bba55d77f','pass','201102','CPAN.DCOLLINS@comcast.net','Abstract-Meta-Class','0.11','i686-linux-thread-multi','5.11.0 patch GitLive-blead-163-g28b1dae','linux','2.6.24-19-generic','201102010041',2
    );
    $ct->repeat_query( $sql );
    $ct->repeat_query();
    @arr = $ct->get_query('array','SELECT count(*) FROM cpanstats');
    is($arr[0]->[0], 8, '.. count all records before repeater');
    $ct->repeat_queries();
    @arr = $ct->get_query('array','SELECT count(*) FROM cpanstats');
    is($arr[0]->[0], 11, '.. count all records after repeater');
    $ct->repeat_queries();
    @arr = $ct->get_query('array','SELECT count(*) FROM cpanstats');
    is($arr[0]->[0], 11, '.. count all records after repeater');

    my $rowid = $ct->do_query();
    is($rowid, undef, '.. blank sql - no row id');


    # insert using auto increment
    SKIP: {
        skip "skipping MySQL tests", 3  unless($opts{driver} eq 'mysql');

        $sql = 'INSERT INTO cpanstats ( guid, state, postdate, tester, dist, version, platform, perl, osname, osvers, fulldate, type) VALUES ( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ? )';
        my $id = $ct->id_query( $sql,'2967432-ed372d00-b19f-3f77-b713-d32bba55d77f','fail','201102','andreas.koenig.gmwojprw@franz.ak.mind.de','Acme-CPANAuthors-French','0.07','x86_64-linux','5.10.0','linux','2.6.24-1-amd64','201102011038',2);
#diag("id=$id");
        ok($id,'.. got back an id');
        @arr = $ct->get_query('hash','SELECT guid FROM cpanstats WHERE id=?',$id);
        is($arr[0]->{guid}, '2967432-ed372d00-b19f-3f77-b713-d32bba55d77f', '.. added record');
        @arr = $ct->get_query('array','SELECT count(*) FROM cpanstats');
        is($arr[0]->[0], 12, '.. inserted all records');
#diag(Dumper(\@arr));
    }

    # test quote
    my $text = "Don't 'Quote' Me";
    like($ct->quote($text), qr{'Don(\\'|'')t (\\'|'')Quote(\\'|'') Me'}, '.. quoted');

    # clean up
    $td->{driver}->drop_database($td->name);
}

sub create_sqlite_databases {
    my $db = shift;

    my @create_cpanstats = (
        'PRAGMA auto_vacuum = 1',
        'DROP TABLE IF EXISTS cpanstats',
        'CREATE TABLE cpanstats (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            guid        TEXT,
            state       TEXT,
            postdate    TEXT,
            tester      TEXT,
            dist        TEXT,
            version     TEXT,
            platform    TEXT,
            perl        TEXT,
            osname      TEXT,
            osvers      TEXT,
            fulldate    TEXT,
            type        INTEGER)',

        'CREATE INDEX distverstate ON cpanstats (dist, version, state)',
        'CREATE INDEX ixguid ON cpanstats (guid)',
        'CREATE INDEX ixperl ON cpanstats (perl)',
        'CREATE INDEX ixplat ON cpanstats (platform)',
        'CREATE INDEX ixdate ON cpanstats (postdate)',
    );

    dosql($db,\@create_cpanstats);
}

sub create_mysql_databases {
    my $db = shift;

    my @create_cpanstats = (
        'DROP TABLE IF EXISTS cpanstats',
        q{CREATE TABLE `cpanstats` (
            `id`        int(10) unsigned NOT NULL AUTO_INCREMENT,
            `guid`      varchar(64)     NOT NULL DEFAULT '',
            `state`     varchar(32)     DEFAULT NULL,
            `postdate`  varchar(8)      DEFAULT NULL,
            `tester`    varchar(255)    DEFAULT NULL,
            `dist`      varchar(255)    DEFAULT NULL,
            `version`   varchar(255)    DEFAULT NULL,
            `platform`  varchar(255)    DEFAULT NULL,
            `perl`      varchar(255)    DEFAULT NULL,
            `osname`    varchar(255)    DEFAULT NULL,
            `osvers`    varchar(255)    DEFAULT NULL,
            `fulldate`  varchar(32)     DEFAULT NULL,
            `type`      int(2)          DEFAULT '0',
            PRIMARY KEY (`id`)
        )},

        'CREATE INDEX distverstate ON cpanstats (dist, version, state)',
        'CREATE INDEX ixguid ON cpanstats (guid)',
        'CREATE INDEX ixperl ON cpanstats (perl)',
        'CREATE INDEX ixplat ON cpanstats (platform)',
        'CREATE INDEX ixdate ON cpanstats (postdate)',
    );

    dosql($db,\@create_cpanstats);
}

sub dosql {
    my ($db,$sql) = @_;

    for(@$sql) {
        #diag "SQL: [$db] $_";
        eval { $db->dbh->do($_); };
        if($@) {
            diag $@;
            return 1;
        }
    }

    return 0;
}
