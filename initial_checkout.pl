#!/perl/bin/perl

use strict;
use warnings;
use File::Basename ();

my $baseurl = 'http://cpan.cs.utah.edu';

my $base_path = shift @ARGV or die("Need a base path.");

# -c means just download the tarball
my $cache_only = 1 if (grep {$_ eq '-c'} @ARGV );

my $distros_dir = "$base_path/distros";
mkdir $distros_dir;
-d $distros_dir or die($?);

my $cache_dir = "$base_path/tar_cache";
mkdir $cache_dir;
-d $cache_dir or die($?);

print "Writing out distros to $distros_dir\n";

chdir($base_path) or die($!);
my $packages = get_packages_info();

foreach my $distro_hash (sort { $a->{'distro'} cmp $b->{'distro'}} values %$packages) {
    my $distro = $distro_hash->{'distro'};
    my $author_path = $distro_hash->{'author_path'};
    my $file = $distro_hash->{'file'};

    my $url = "$baseurl/authors/id/$author_path";
        

    # Stage the tarball.
    my $cache_file = "$cache_dir/$file";
    unless (-f $cache_file && !-z _) {
        DEBUG("Retrieving $distro from $url");
        my $wget_got = `wget -nv --no-use-server-timestamps -P $cache_dir --unlink "$url" 2>&1`;
        my $wget_error = $?;
        if (!-e $cache_file) {
            DEBUG("Failed to download $file ($wget_error).");
            DEBUG($wget_got);
        }
    }

    -f $cache_file && !-z _ or die("Can't download or find $cache_file");
    next if $cache_only; # Just download the tarball
    #next unless ($distro =~ m/^[a]/i); # If you want to only parse some modules.
    
    my $distro_base = "$distros_dir/$distro";
    if( -d $distro_base) {
        #DEBUG("Skipping $distro (already there!)");
        next;
    }
    
    DEBUG("Extracting $distro ($cache_file)");
    mkdir($distro_base) or die($!);
    chdir $distro_base;    

    extract($cache_file, $distro_base);
    #`git add -A .`;
}

exit;

sub extract {
    my ($cache_file, $distro_base) = @_;
    $cache_file or die;
    $distro_base or die;

    my $tool;
    $tool = '/usr/bin/tar -xf' if $cache_file =~ m/\.tar\.(gz|bz2)$|\.tgz$/i;
    $tool = '/usr/bin/unzip' if $cache_file =~ m/\.zip$/i;
    return if $cache_file =~ m/\Q.pm.gz\E$/i;
    $tool or die("Don't know how to handle $cache_file");
    
    my $untar_got = `$tool "$cache_file" 2>&1`;
    my $untar_error = $?;
    
    if ($untar_error) {
        DEBUG("Error extracting $cache_file ($untar_error)");
        DEBUG($untar_got);
        return $untar_got;
    }
    
    my $dir = File::Basename::fileparse($cache_file);
    while ( $dir && !-d $dir) {
        chop $dir;
    }
    if (!$dir) {
        my @files = glob ('*');
        if (scalar @files == 1 && -d $files[0]) {
            $dir = $files[0];
            $dir && $dir !~ m/^\./ or die("Unexpected dir $dir");
        }
        elsif(scalar @files) {
            DEBUG("$cache_file ($dir) had no base dir????");
            return 0;
        }
        else {
            DEBUG("XXXX Could not find a dir ($dir) for $cache_file");
            return -1;
        }
    }
    
    `mv $dir/* $distro_base 2>&1`;
    `/bin/rm -rf "$dir/.git" 2>&1`; # Just in case.
    `mv $dir/.* $distro_base 2>&1`;
    `find "$distro_base" -name .git -exec /bin/rm -rf {} \\; 2>&1`; # remove extracted .git dirs.
    
    my @big_files = `find $distro_base -type f -size +5M`;
    foreach my $bigfile (@big_files) {
        chomp $bigfile;
        next if $bigfile =~ m{/sqlite3.c$}; # We're going to white list sqlite3.c
        open(my $fh, '>', $bigfile) or die ("Can't write to $bigfile?? $?");
        print {$fh} "The contents of this file exceeded 5MB and were deemed inappropriate for this repository.\n";
    }

    rmdir $dir;
    if(-d $dir) {
        DEBUG("Failed to move files located in $dir to the base path for $cache_file");
        return -2;
    }

    return 0;
}

sub get_packages_info {
    my $otwofile = '02packages.details.txt';

    if (!-e $otwofile) {
        my $url = "$baseurl/modules/$otwofile.gz";
        print "Retrieving and parsing $url\n";
        print `wget --unlink "$url" 2>&1`;
        print `gunzip "$otwofile.gz" 2>&1`;
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
        $distro =~ s/-20130623.tgz$//; # triceps-1.0.93-20130623.tgz
        $distro =~ s/-100701.tar.gz$//; # Zobel-0.20-100701.tar.gz
        $distro =~ s/-(OpenSource|FIXED|fix).tar.gz$/.tar.gz/; # NewsClipper-1.32-OpenSource.tar.gz Net-SMS-2Way-0.08-FIXED.tar.gz
        $distro =~ s/-(PPM).zip$/.zip/; # Net-IPAddress-1.10-PPM.zip
        $distro =~ s/_Feb-27-2013.tgz$/.tgz/; # HTML-WikiConverter-DokuWikiFCK-0.32_Feb-27-2013.tgz
        
        # Custom
        $distro =~ s/Ext1-0-07.tgz$/Ext1-7.tgz/; # Math-MatrixReal-Ext1-0-07.tgz
        $distro =~ s/-B.tar.gz$/.tar.gz/; # Zonemaster-GUI-1.0.7-B.tar.gz

        $distro =~ s/-\d\.\d{1,3}-\d\.tar\.gz$// or # Double dash pattern alternative to -XXX.tar.gz
        $distro =~ s/-[^-]+?$//; # Strip off version and tar extension.
        $distro =~ s/-\d\.\d{1,3}$//; # Extra version info in a 1.22 format so obvious?
        $distro =~ s/-\d\.\d{1,3}\.\d{1,3}$//; # Extra version info in a 1.2.2 format so obvious?
        $distro =~ s/_v?\d.*.tar.gz$//; # _ 
        next if ($distro =~ m/\.gz$/); # It's just not in distro format. We tried! now it's time to give up.

        # Skip if we have a newer version for $distro already.
        next if( $packages->{$distro} && version->parse($version) < version->parse($packages->{$distro}->{'version'}));
    
        $file =~ m/"/ and die("$file unexpectedly had a \" in it??");
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