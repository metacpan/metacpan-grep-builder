#!/perl/bin/perl

use strict;
use warnings;
use File::Basename ();
use CPAN::Meta::YAML ();
use CPAN::DistnameInfo ();
use POSIX ":sys_wait_h";
use version ();

my $allowed_parallel_downloads = 20;
my $baseurl = 'http://cpan.cs.utah.edu';

my $base_path = shift @ARGV or die("Need a base path.");

my $distros_dir = "$base_path/distros";
mkdir $distros_dir;
-d $distros_dir or die($?);

my $distro_meta_dir = "$base_path/distro_meta";
mkdir $distro_meta_dir;
-d $distro_meta_dir or die($?);
my $tarball_parsed_file = "$distro_meta_dir/parsed.txt";

my $cache_dir = "$base_path/tar_cache";
mkdir $cache_dir;
-d $cache_dir or die($?);

my $temp_dir = "$base_path/tmp";

my $validate_tarballs = (grep {$_ eq '-v'} @ARGV) ? 1 : 0;
print "VALIDATING ON\n" if $validate_tarballs;
DEBUG("Staging tarballs...");
stage_tarballs();
if (grep {$_ eq '-c'} @ARGV ) {
    DEBUG("Exiting due to cache only request");
    exit;
}

print "Writing out distros to $distros_dir\n";
chdir($base_path) or die($!);

my %processed_tarball;
my $fh = open_otwo_packages_file();
while (my $line = <$fh>) {
    my ($module, $module_version, $author_path) = split(qr/\s+/, $line);
    next if $author_path =~ m{\.pm\.gz$};
    next if $author_path =~ m{/Bundle-FinalTest2.tar.gz$};
    next if $author_path =~ m{/Spreadsheet-WriteExcel-WebPivot2.tar.gz$};
    next if $author_path =~ m{/perl5.00402-bindist04-msvcAlpha.tar.gz$};
    next if $author_path =~ m{/Geo-GoogleEarth-Document-modules2.tar.gz$};
    
    chomp $author_path;
    my $tarball_file = path_to_tarball_cache_file($author_path);
        
    next if $processed_tarball{$tarball_file};
    $processed_tarball{$tarball_file} = 1;
    -f $tarball_file && !-z _ or die("ZERO TARBALL??? $tarball_file");
    
    next if was_parsed($author_path);

    DEBUG("Parsing $author_path");
    my $extracted_distro_name = expand_distro($tarball_file, $author_path);
}

exit;

my %tarball_parsed_cache;
sub was_parsed {
    my $author_path = shift or die;
    return 1 if $tarball_parsed_cache{$author_path};
    open(my $fh, '<', $tarball_parsed_file) or return 0;
    while (<$fh>) {
        chomp;
        $tarball_parsed_cache{$_} = 1;
    }

    return $tarball_parsed_cache{$author_path};
}

sub path_to_tarball_cache_file {
    my $author_path = shift or die;
    $author_path =~ s{/}{-}g;
    return "$cache_dir/$author_path";
}

my %parallel_download_pids;
my %tarball_requested;
sub stage_tarballs {
    my $fh = open_otwo_packages_file();
    
    while (my $line = <$fh>) {
        my ($module, $module_version, $author_path) = split(qr/\s+/, $line);
        chomp $author_path;
                
        my $url = "$baseurl/authors/id/$author_path";
        
        my $tarball_file = path_to_tarball_cache_file($author_path);
        
        next if $tarball_requested{$tarball_file};
        $tarball_requested{$tarball_file} = 1;

        if( -f $tarball_file and !-z _) {
            next unless $validate_tarballs;
            next if ($tarball_file !~ m/\.tar\.gz$/);
            
            my $errors = `tar -tzf $tarball_file 2>&1`;
                
            if ($?) {
                print "Got errors validating $tarball_file with $? and $errors\n";
                unlink $tarball_file;
            }
            else {
                next;
            }
        }
        
        unlink $tarball_file;
        DEBUG("Retrieving $tarball_file from $url");

        print scalar(keys(%parallel_download_pids)) . " parallel procs\n";
        while (scalar keys %parallel_download_pids > $allowed_parallel_downloads) {
            my $kid = waitpid(-1, WNOHANG);
            delete $parallel_download_pids{$kid};
        }

        my $pid = fork();
        if($pid) {
            $parallel_download_pids{$pid} = 1;
        }
        elsif (!defined $pid) {
            die("fork failed!");
        }
        elsif ($pid == 0) {
            my $todo = qq{wget -nv --no-use-server-timestamps -O "$tarball_file" --unlink "$url" 2>&1};
            my $wget_got = exec($todo);
            exit;
        }
    }
    
    # Cleanup.
    while (scalar keys %parallel_download_pids > 0) {
        my $kid = waitpid(-1, WNOHANG);
        delete $parallel_download_pids{$kid};
    }

    my $redo = 0;    
    foreach my $zero_file (keys %tarball_requested) {
        next unless -z $zero_file;
        delete $tarball_requested{$zero_file};
        $redo++;
        unlink $zero_file;
    }
    
    close $fh;
    
    return if !$redo;

    print "\n\n$redo zero tarballs. Looping!\n";
    sleep 5;
    goto &stage_tarballs;
}

sub expand_distro {
    my ($tarball_file, $author_path) = @_;
    $tarball_file or die;
    $author_path or die;

    my $tool;
    $tool = '/usr/bin/tar -xf' if $tarball_file =~ m/\.tar\.(gz|bz2)$|\.tgz$/i;
    $tool = '/usr/bin/unzip' if $tarball_file =~ m/\.zip$/i;
    $tarball_file =~ m/\Q.pm.gz\E$/i and die(".pm.gz unsupported!");
    $tool or die("Don't know how to handle $tarball_file");
    
    `/bin/rm -rf "$temp_dir" 2>&1`; # Just in case.
    mkdir($temp_dir) or die("Couldn't create temp dir $temp_dir");
    chdir $temp_dir or die;
    
    my $untar_got = `$tool "$tarball_file" 2>&1`;
    my $untar_error = $?;
    
    if ($untar_error) {
        DEBUG("Error extracting $tarball_file ($untar_error)");
        DEBUG($untar_got);
        return $untar_got;
    }
    
    # Collapse al 
    my $dir = File::Basename::fileparse($tarball_file);
    while ( $dir && !-d $dir) {
        chop $dir;
    }
    if (!$dir) {
        my @files = glob ('*');
        if (scalar @files == 1 && -d $files[0]) {
            $dir = $files[0];
            $dir && $dir !~ m/^\./ or die("Unexpected dir $dir");

            `mv $temp_dir/$dir/* $temp_dir 2>&1`;
            `/bin/rm -rf "$temp_dir/.git" 2>&1`; # Just in case.
            `mv $temp_dir/$dir/.* $temp_dir 2>&1`;
            `find "$temp_dir" -name .git -exec /bin/rm -rf {} \\; 2>&1`; # remove extracted .git dirs.
        }
        elsif(scalar @files) {
            #DEBUG("$tarball_file ($dir) had no base dir????");
        }
        else {
            DEBUG("XXXX Could not find a dir ($dir) for $tarball_file");
            return -1;
        }
    }

    # Zero out the big files.
    my @big_files = `find $temp_dir -type f -size +5M`;
    foreach my $bigfile (@big_files) {
        chomp $bigfile;
        next if $bigfile =~ m{/sqlite3.c$}; # We're going to white list sqlite3.c
        open(my $fh, '>', $bigfile) or die ("Can't write to $bigfile?? $?");
        print {$fh} "The contents of this file exceeded 5MB and were deemed inappropriate for this repository.\n";
    }
    
    # Read the meta file we just extracted and try to determine the distro dir.
    my ($distro, $version) = determine_distro_and_version($temp_dir, $author_path);
    print "$distro -- $version -- $author_path\n" if !$distro;
    my $existing_meta = get_stored_version_info($distro);
    

    if (!$existing_meta or !length $existing_meta->{'version'} or compare($existing_meta->{'version'}, '<', $version)) {
        my $letter = substr($distro, 0, 1);
        mkdir "$distros_dir/$letter";
        my $distro_base = "$distros_dir/$letter/$distro";
        `/bin/rm -rf "$distro_base" 2>&1`;
        mkdir $distro_base;
        system("/usr/bin/mv $temp_dir/*  '$distro_base' >/dev/null 2>&1");
        system("/usr/bin/mv $temp_dir/.* '$distro_base' >/dev/null 2>&1");

        # Write out a data file explaining where we came from.
        write_stored_version_info($distro, $version, $author_path);
    }

    open(my $fh, ">>", $tarball_parsed_file) or die;
    print {$fh} "$author_path\n";
    close $fh;
    
    return 0;
}

sub write_stored_version_info {
    my ($distro, $version, $author_path) = @_;
    $author_path or die("Can't write meta without author_path! $distro $version");
    $distro or die("Can't process $author_path without a distro");
    $version ||= 0;
    
    my $letter = substr($distro, 0, 1);
    mkdir "$distro_meta_dir/$letter";
    -d "$distro_meta_dir/$letter" or die ("Can't create directory $distro_meta_dir/$letter");
    
    my $meta_file = "$distro_meta_dir/$letter/$distro.yml";

    open(my $fh, '>', $meta_file) or die "Can't write $meta_file";
    print {$fh} "---\ndistro: $distro\nversion: $version\nauthor_path: $author_path\n";
    close $fh;  
}

sub get_stored_version_info {
    my $distro = shift or die("No distro passed to get_stored_version_info");
    
    my $letter = substr($distro, 0, 1);
    
    my $meta_file = "$distro_meta_dir/$letter/$distro.yml";
    return if !-e $meta_file or -z _;
    open(my $fh, '<', $meta_file) or return;

    my $hash = {};
    while (my $line = <$fh>) {
        chomp $line;
        next if ($line =~ m/^---/);
        next unless $line =~ m/^(\S+?):\s*(\S+)/;
        $hash->{$1} = $2;
    }
    close $fh;

    return $hash;
}

sub determine_distro_and_version {
    my ($temp_dir, $author_path) = @_;

    my $d = CPAN::DistnameInfo->new($author_path);
    my $distro = $d->dist;
    my $version = $d->version;

    # Is the version parseable?
    if ($version and eval{ version->parse($version) ; 1 } ) {
        return ($distro, $version);
    }
    
    my $new_yaml = "$temp_dir/META.yml";
    open(my $fh, '<', $new_yaml) or return ($distro, $version);
    my ($meta_name, $meta_version);
    while (my $line = <$fh>) {
        chomp $line;
        if ($line =~ m/^name:\s*["']?(\S+)["']?\s*$/) {
            $meta_name = $1;
        }
        if ($line =~ m/^version:\s*["']?(\S+)["']?\s*$/) {
            $meta_version = $1;
        }
        last if(length $meta_name and length $meta_version);
    }
    
    # Couldn't parse meta. Just fall back to CPAN::DistnameInfo
    $meta_name or return ($distro, $version);

    $meta_name =~ s/::/-/g;
    return ($meta_name, $meta_version);
}

sub open_otwo_packages_file {
    my $otwofile = '02packages.details.txt';

    my $local_otwo_file = "$base_path/$otwofile";
    if (!-e $local_otwo_file) {
        my $url = "$baseurl/modules/$otwofile.gz";
        print "Retrieving and parsing $url\n";
        print `wget --unlink "$url" -O '$local_otwo_file.gz' 2>&1`;
        print `gunzip "$otwofile.gz" 2>&1`;
    }
    else {
        print "Cached $local_otwo_file\n";
    }
    
    open(my $fh, '<', $local_otwo_file) or die;

    # Read in and ignore the header.
    my $line = <$fh>;
    while ($line =~ m/\S/) {
        $line = <$fh>;
    }

    return $fh;    
}

sub get_packages_info {
    my $fh = open_otwo_packages_file();
    
    my $packages = {};
    # Which files do we want to download and maintain??
    while (my $line = <$fh>) {
        my ($module, $module_version, $file) = split(qr/\s+/, $line);
        chomp $file;
        next if !length $module_version; # Means we didn't read it in.
        next if !length $file; # Means we didn't read it in.
        next if $module_version eq 'undef';
        next if ($file =~ m/\.pm\.gz$/i);
    
        my $distro_file = $file;
    
        $distro_file =~ s/\.pm\.gz$//; # https://github.com/andk/pause/issues/237
        
        my $d = CPAN::DistnameInfo->new($distro_file);
        my $distro = $d->dist || $distro_file;
        my $version = $d->version || $module_version;

        # Skip if we have a newer version for $distro already.
        next if( $packages->{$distro} && compare($packages->{$distro}->{'version'}, '>=', $version) );
    
#        $file =~ m/"/ and die("$file unexpectedly had a \" in it??");
        # Store it.
        $packages->{$distro} = {
            author => $d->cpanid,
            version => $version,
            author_path => $file,
            file => File::Basename::fileparse($file),
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

sub compare {
    my ( $left, $right ) = align( $_[0], $_[2] );
    my $cmp = $_[1];

    return $left gt $right if ( $cmp eq '>' );
    return $left lt $right if ( $cmp eq '<' );
    return ( $left gt $right or $left eq $right ) if ( $cmp eq '>=' );
    return ( $left lt $right or $left eq $right ) if ( $cmp eq '<=' );
    return $left eq $right if ( $cmp eq '=' );

    die("Unknown comparison: '$cmp' -- $_[0] $_[1] $_[2]");
}

sub align {
    my ( $left, $right ) = @_;
    $left  = '' if ( !$left );
    $right = '' if ( !$right );

    # Leading/trailing whitespace.
    $_ =~ s/^\s+// foreach ( $left, $right );
    $_ =~ s/\s+$// foreach ( $left, $right );

    $left  = "0:$left"  if ( $left !~ m/^\d*:/  && $right =~ /^\d*:/ );    # Insert 0 epoch if not on both.
    $right = "0:$right" if ( $right !~ m/^\d*:/ && $left =~ /^\d*:/ );     # Insert 0 epoch if not on both.

    for ( $left, $right ) {
        $_ =~ "$_-0" if ( $_ !~ m/-\d+$/ );                                # Force a -0 version on each, similar to forcing an epoch.
    }

    # Split
    my (@left_array)  = split( /[\.\-\:]/, $left );
    my (@right_array) = split( /[\.\-\:]/, $right );

    # Pad each section with zeros or spaces.
    for my $seg ( 0 .. $#left_array ) {
        $right_array[$seg] = 0 if ( !$right_array[$seg] );                 # In case right is not set.

        my ( $left_len, $right_len ) = ( 0, 0 );
        $left_array[$seg] =~ m/^(\d+|^\D+)/
          and $left_len = length($1);
        $right_array[$seg] =~ m/^(\d+|^\D+)/
          and $right_len = length($1);

        if ( $left_len < $right_len ) {
            my $appender = $left_array[$seg] =~ m/^\d/ ? '0' : ' ';
            $left_array[$seg] = $appender x ( $right_len - $left_len ) . $left_array[$seg];
        }
        elsif ( $left_len > $right_len ) {
            my $appender = $right_array[$seg] =~ m/^\d/ ? '0' : ' ';
            $right_array[$seg] = $appender x ( $left_len - $right_len ) . $right_array[$seg];
        }
    }

    # Right segments length is > left segments length?
    for my $seg ( scalar @left_array .. $#right_array ) {
        $left_array[$seg] = "0" x length("$right_array[$seg]");
    }

    return ( join( "~", @left_array ), join( "~", @right_array ) );
}