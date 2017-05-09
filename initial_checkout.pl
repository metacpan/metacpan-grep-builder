#!/perl/bin/perl
use strict;
use warnings;

my $baseurl = 'http://cpan.cs.utah.edu';

my $base_path = shift @ARGV or die("Need a base path.");
my $distros_dir = "$base_path/distros";
mkdir $distros_dir;
-d $distros_dir or die($?);

print "Writing out distros to $distros_dir\n";

chdir($base_path) or die($!);
my $packages = get_packages_info();

foreach my $distro_hash (sort { $a->{'distro'} cmp $b->{'distro'}} values %$packages) {
    my $distro = $distro_hash->{'distro'};
    my $author_path = $distro_hash->{'author_path'};

    my $distro_base = "$distros_dir/$distro";
    if( -d $distro_base) {
        DEBUG("Skipping $distro (already there!)");
        next;
    }
    mkdir($distro_base) or die($!);
    chdir $distro_base;
    my $url = "$baseurl/authors/id/$author_path";

    DEBUG("Retrieving $distro from $url");
    my $wget_got = `wget --unlink $url 2>&1`;
    my $wget_error = $?;
    
    my $file = $distro_hash->{'file'};

    if(!-e $file) {
        chdir $distros_dir;
        DEBUG("Cleaning up $distro_base");
        `/bin/rm -rf $distro_base`;
        DEBUG("Failed to download $file ($wget_error).");
        DEBUG($wget_got);
        next;
    }
    
    extract($file);
    `git add .`;
}

exit;

sub extract {
    my $file = shift or die;
    
    my $tool = '/usr/bin/tar -xf';
    $tool = '/usr/bin/unzip' if $file =~ m/\.zip$/i;
    
    my $untar_got = `$tool $file`;
    my $untar_error = $?;
    
    if ($untar_error) {
        DEBUG("Error extracting $file ($untar_error)");
        DEBUG($untar_got);
        return $untar_got;
    }
    
    unlink $file;
    
    my $dir = $file;
    while ( $dir && !-d $dir) {
        chop $dir;
    }
    if (!$dir or !-d $dir) {
        if (! scalar glob('*')) {
            DEBUG("Could not find a dir ($dir) for $file");
            return -1;
        }
        DEBUG("XXX $file had no base dir????");
        return 0 if !$dir; # There are files here so it extracted with no base dir. so we're good??
    }
    
    `mv $dir/* .`;  
    `mv $dir/.* . 2>&1`;
    rmdir $dir;
    if(-d $dir) {
        DEBUG("Failed to move files located in $dir to the base path for $file");
        return -2;
    }

    return 0;
}

sub get_packages_info {
    my $otwofile = '02packages.details.txt';

    if (!-e $otwofile) {
        my $url = "$baseurl/modules/$otwofile.gz";
        print "Retrieving and parsing $url\n";
        print `wget --unlink $url 2>&1`;
        print `gunzip $otwofile.gz 2>&1`;
    }
    else {
        print "Cached $otwofile\n";
    }
    
    open(my $fh, '<', $otwofile) or die;

    # Read in and ignore the header.
    my $line = <$fh>;
    while ($line =~ m/\S/) {
        $line = <$fh>;
    }

    my $packages = {};
    # Which files do we want to download and maintain??
    my ($module, $version, $file);
    while ($line = <$fh>) {
        ($module, $version, $file) = split(qr/\s+/, $line);
        chomp $file;
        next if !length $version; # Means we didn't read it in.
        next if !length $file; # Means we didn't read it in.
        next if $version eq 'undef';
    
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
            author_path => $file,
            file => $filename,
            distro => $distro,
        };
    }
    return $packages;
}

sub DEBUG {
    my $msg = shift;
    chomp $msg;
    print $msg . "\n";
}