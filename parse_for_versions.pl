#!/perl/bin/perl
use strict;
use warnings;

use CPAN::DistnameInfo ();

my $packages = {};
open(my $fh, '<', '/perl/minicpan_grep/02packages.details.txt') or die;

# Read in and ignore the header.
my $line = <$fh>;
while ($line =~ m/\S/) {
    $line = <$fh>;
}

# Which files do we want to download and maintain??
while ($line = <$fh>) {
    my ($module, $module_version, $file) = split(qr/\s+/, $line);
    chomp $file;
    next if !length $module_version; # Means we didn't read it in.
    next if !length $file; # Means we didn't read it in.
    next if $module_version eq 'undef';
    
    my $distro_file = $file;
    
    $distro_file =~ s/\.pm\.gz$//; # https://github.com/andk/pause/issues/237

    #next if !$version;
    
    my $d = CPAN::DistnameInfo->new($distro_file);
    my $distro = $d->dist || $distro_file;
    my $version = $d->version;

    length($distro) or die "$file";
    $version = $module_version if !length $version;
    next if ($version =~ m/^[1-9][0-9]*$/);
    next if (eval { version->parse($version); 1} );
    
    if ($version) {
        my (@v) = split(qr/[_.+-]/, $version);
        $version = join (".", @v); 
        printf("%40s == %20s == %s\n", $version, $distro, $file);
    }
    else {
        print "XXXX $file\n";
    }
    
    
    next;

    # Skip if we have a newer version for $distro already.
    #next unless $version;
    #my $parsed_new_version = version->parse($version);
    #next if( $packages->{$distro} && $parsed_new_version < version->parse($packages->{$distro}->{'version'}));
    
    # Store it.
    $packages->{$distro} = {
#        author => $author,
        version => $version,
        file => $file,
        distro => $distro,
    };
}

use Data::Dumper; $Data::Dumper::Sortkeys = 1;
print Dumper $packages;