#!/perl/bin/perl



my $packages = {};
open(my $fh, '<', '/root/.cpan/sources/modules/02packages.details.txt') or die;

# Read in and ignore the header.
my $line = <$fh>;
while ($line =~ m/\S/) {
    $line = <$fh>;
}

# Which files do we want to download and maintain??
my ($module, $version, $file);
while ($line = <$fh>) {
    ($module, $version, $file) = split(qr/\s+/, $line);
    chomp $file;
    next if !length $version; # Means we didn't read it in.
    next if !length $file; # Means we didn't read it in.
    next if $version eq 'undef';

    #next if !$version;
    #print $line  if $file !~ m/\Q$version\E/; # If the version doesn't match the file version, then this isn't the main package in the module for sure.
    
    my (undef, undef, $author, @path) = split("/", $file);
    my $filename = pop @path;

    # Determine main package name
    my $distro = $filename;
    $distro =~ s/-[^-]+?$//; # Strip off version and tar extension.

    # Skip if we have a newer version for $distro already.
    next if( $packages->{$distro} && version->parse($version) < version->parse($packages->{$distro}->{'version'}));
    
    # Store it.
    $packages->{$distro} = {
        author => $author,
        version => $version,
        file => $file,
        distro => $distro,
    };
}

use Data::Dumper; $Data::Dumper::Sortkeys = 1;
print Dumper $packages;