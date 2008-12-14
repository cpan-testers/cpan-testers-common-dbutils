use ExtUtils::MakeMaker;

WriteMakefile(
    'NAME'          => 'CPAN::Testers::Common::DBUtils',
    'VERSION_FROM'  => 'lib/CPAN/Testers/Common/DBUtils.pm',
    'PREREQ_PM'     => {

        Carp                    => 0,
        Class::Accessor::Fast   => 0,
        DBI                     => 0,
    },

    NO_META         => 1,

    ($] >= 5.005 ?    ## Add these new keywords supported since 5.005
      (ABSTRACT     => 'Basic Database Wrapper',
       AUTHOR       => 'Barbie <barbie@cpan.org>') : ()),
);
