#!/usr/bin/perl

use utf8;
use warnings;
use strict;

use JSON::PP qw( decode_json encode_json );
use HTTP::Tiny;

open( my $client_stats, '>:utf8', 'filp_client_stats.csv') or die $!;
open( my $miner_stats, '>:utf8', 'filp_miner_stats.csv') or die $!;

my $api_url = "https://api.node.glif.io/rpc/v0";

my $ua = HTTP::Tiny->new();
my $filp_deals = decode_json( $ua->get("https://marketdeals.s3.amazonaws.com/StateMarketDealsFilPlusOnly.json")->{content} );

#
# Sourced by various means, not complete
#
my $miners = {
  f01234 => { owner => "ElioVP"  },
  f01240 => { owner => "Speedium" },
  f01276 => { owner => "NBFS" },
  f01278 => { owner => "stuberman" },
  f02401 => { owner => "NBFS" },
  f02576 => { owner => "benjaminh" },
  f07998 => { owner => "Anwen" },
  f08403 => { owner => "TippyFlits" },
  f09848 => { owner => "bigbear" },
  f010479 => { owner => "s0nik42" },
  f010617 => { owner => "Kernelogic" },
  f015927 => { owner => "Chris" },
  f019104 => { owner => "NBFS" },
  f022142 => { owner => "Nelson" },
  f022352 => { owner => "reiers" },
  f023467 => { owner => "Phi Mining" },
  f034544 => { owner => "unknown cn 11" },
  f034545 => { owner => "unknown cn 12" },
  f047419 => { owner => "deegee" },
  f049882 => { owner => "Chipz" },
  f053229 => { owner => "unknown 1" },
  f062353 => { owner => "Factor8" },
  f063869 => { owner => "unknown cn 1" },
  f064218 => { owner => "XinAn Xu" },
  f089228 => { owner => "unknown 2" },
  f0116436 => { owner => "unknown cn 2" },
  f0116445 => { owner => "unknown cn 3" },
  f0118317 => { owner => "unknown cn 4" },
  f0118330 => { owner => "unknown cn 5" },
  f0134516 => { owner => "unknown cn 6" },
  f0134518 => { owner => "unknown cn 7" },
  f0161542 => { owner => "unknown 3" },
  f0167505 => { owner => "unknown cn 8" },
  f0226324 => { owner => "unknown cn 9" },
  f0228712 => { owner => "unknown na 1" },
  f0364957 => { owner => "unknown cn 10" },
  f0391143 => { owner => "unknown cn 11" },
};

#
# Sourced from the two public registries:
# https://github.com/keyko-io/filecoin-clients-onboarding
# https://github.com/filecoin-project/filecoin-plus-client-onboarding
#
my $clients = {
  "!!!ALLOCATION NOT PUBLIC!!!" => { addresses => [
    "f12fgw47xvk5k3saaqxg5laglbkjsrg53qumrqxai",
    "f13cigaqtj7bsonrx6rggjlipcvcdltq27eyls3pq",
    "f13qfmnpzwcwmcozifakpvp4gjvqp74pmkk3r5s7a",
    "f1555tn7gebk5iiupmmre7ktwwomirh7r36wxzr6a",
    "f15sad5ij57vkegceldbgtdsg37kgdtc3isicq5ya",
    "f16bt3r6osr4qe27bvhbq5nwss25q4s2xuiydr43a",
    "f17bivwapr6o4y3wxg4cofoy2c5bjiqc2xwwyuyda",
    "f1banh3vlhmi7qz2ob5l4xexcujvpozk2yl5lhkby",
    "f1bisba2smhkdabtw2blvfakassme6vtfdwloivra",
    "f1bw7k6shtjntgzk4fspnb47m57roiirqqudsi4xq",
    "f1eskgtm4frbomk2grlvthth7shex5wiledcochjy",
    "f1fanfgu4wocton6beiuqfoznrou2oxt7hcbgcupy",
    "f1h6fepddlm35b7bkveoz3ugqsrrydpbzjhbno7di",
    "f1hjpysclhuxkpezt5ktqnj7yfppljakrg3otlgza",
    "f1iay4rym7fut3sxwrwvzdgmflz7n4xr4e4k5un7a",
    "f1jrbkeegk6v7ezfbtgyh4kxpk6j6tflulpyvpuly",
    "f1p3risbnen7wx5y5kvlbxhx74lg4csgntyj73kuy",
    "f1po5roufdxrf3blqax7baks4xbbs6rne6r3eqixq",
    "f1rahol4tkwpyt5ik4mij3gxr5pj5xk5u3p2kb5iy",
    "f1sqtwgzh6wcolfkap5y6ftk4qbbc4y4jvcotfhuy",
    "f1y6py4yuc2jwukmeivurhrwzffqpmqgmdboggdfy",
    "f3tgmml5p6i3jq3ze6sm3toqgz7pbx6eepdoegysa3lh7ws355salecx24vspt7d4xvjifyhmyzr6dpvawmifq",
    "f3ulwcfkdp7cqtvyfrfigr6yjmpil5bpas2w7q5iw7ck4dlmuquybvwnbct3yhfjmdbkj7xjdiblhmxgwg5yqq",
    "f3wd5yqgo7tlb2b3yzfuwl63h6u64wsp7i7t5jon7umwye34w73oicqhdjo7vavfoevq5x3d5esrjf3nb4u27a",
    "f3wopdmnfpq2rgjqeguqwodv6gzfjukr5f3ybnsxdx5vzvxvs62z3i2kjaamjxl4uju7rn332g6oecsk5stlfa",
    "f3wq7hcex7elk6qcfgnmyetafwoixpk2csxsqw2oecpswsh4th66ux3o4od3chu73jiunef2t564yglb6nyuhq",
    "f3wwoynibkh6ypcz7hwlepzdld3kkobdtbz6bkufrh3jz56ztjog7ysuisn5ykrdegomsetrh6heodbalqslwq",
  ]},
  "Alyale Philippines" => { addresses => [
    "f3qqhbwhgorbtymzhq5wxnointwl4tl5riqvxv35utyuhlisbz4247io32oshff2emysu5fzc2t4msh64ahlpq",
  ]},
  Askender => { addresses => [
    "f3uvwjknloel3zcmjrgrbmgqwp3c32l7b7tyoqj7p7njb3c2xmex6dh2jzz45a6smzqvhw53nbng2zn2p7ciza",
  ]},
  ElioVP => { addresses => [
    "f3rnzgvmh6itjywfy3n6e23s3impootcafbook4x3et5qqmxrces6hd5oypufdwkwi6eobnhfqny3oohr2s5ea",
  ]},
  Filbox => { addresses => [
    "f3umho76jhnreafz5nraeyob25kidea26xz5xzewpe5glc45ullz55bzusfnfrk2nlb3ibd3aiphhmblqpuvda",
  ]},
  FileDrive => { addresses => [
    "f3s3dwh67ps4kxui4mziwvus5ue7p6p324aiebrrpe24st5t334ib5gwlxql6auo7ibrccmppjxxavah46wdqa",
    "f3wgfwtrs5p6jrkwfl2mksqa2ivgbgdjjrhjbefy3n7qzvotc3y6sazmp5gfyj7um6jlgdvlbiepzawnc6wxtq",
  ]},
  "Guazi Dynamic" => { addresses => [
    "f3qaipvxrz2gxexc7mcvjsxifmscfiw7c7zfhrmq76j5ee4hcbvg3gbtpea7wgz72kkjcjmwzhm5uo2onxyocq",
    "f3wvmap64wokgkvavberi2l3o7li5egvym526kdgk3bmnmto5qbcq7jin3wpbkiifoa26caxreois237d66nrq",
  ]},
  "IPFS Force" => { addresses => [
    "f3rtqrpxadmhkfgy3cmtib4q6pwiqxpn6fxxq7kvopwqtwghwhwhyy7lm4euqvkg5h26ll3fpc54zsv7jtwdna",
  ]},
  "IPFS Union" => { addresses => [
    "f3rf334djo4e76f44hz6tbkh5maerg42nr5b5squsb7kty336zv6pdnnwhyc2qaijpq2dwkpyy3fwfeofjtpja",
  ]},
  JV => { addresses => [
    "f1s566sc7nmxo7b7qml7m6s72uwmljg6k5orliolq",
  ]},
  Kernelogic => { addresses => [
    "f3rfb7aarwmrulwc4rqy4526zvopnsnbsmcfokhbi743kq2shaybo46cbjqezvmhhjkejpk6xqbafixvplq5xa",
    "f3viailjjkez5o5d3p2flxkdkag2srqy3wouaxcemabrqjp2nr6l56yxbx5pu3qgszgo7dgrzpon4xpuweny5a",
  ]},
  LandSatFile => { addresses => [
    "f3rikexkas2vxfhc6nbhla7hpunrp6as5msjx3wwndve6rfgzmyrliu2p3ypzdf74yzfwjhyqpnt6gj4xwzkoa",
  ]},
  "Linix Webhosting" => { addresses => [
    "f1jqjoleqxffusnsyxxlmyedhqsiigoqg2eg4sgzq",
  ]},
  NBFS => { addresses => [
    "f3rkcl5lnoumsunl3grsy2b57z7zt6uoyvgnviz2dw3ir7sslnya2634as2ksfxzziuxzgubavqsb3s2fmofcq",
    "f3ufzpudvsjqyiholpxiqoomsd2svy26jvy4z4pzodikgovkhkp6ioxf5p4jbpnf7tgyg67dny4j75e7og7zeq",
  ]},
  "Neo Beat" => { addresses => [
    "f3uqb2gqegru5ycchufy6gyjbpiu3dk3tx4nk45aua6wd6krbvkxyj6qapu6kkicm7nyzpdpg22clgwwy2wwba",
  ]},
  "Phi Mining" => { addresses => [
    "f3v2bujmjhxugk3wtwwqha3za7vxmetyvd5rfj2dxff7zekibcclmbhd2ytrmrfls7bb2bz53ehcpl5npss5hq",
  ]},
  "Piñata" => { addresses => [
    "f17kamajrdzjcjzj6b3y2ovc64kc7qxvkbx2dzvza",
  ]},
  "Shanghai Futures Exchange" => { addresses => [
    "f1jp2xt7qkgdfmsj6pylcalmr7t4atfby2aoyeh4i",
  ]},
  "Shenzhen Xiaoyu Blockchain Technology" => { addresses => [
    "f1ry5tgrth2qt4r5zu4e6ssfkf4qrce7hcdp22kbi",
  ]},
  Simba => { addresses => [
    "f3srltdd7fvnukjpywap5wksbpmdenhvwr6imza3nxlxycmmi2lit2kbisij5wly5fmjwcatmetyktpd5tfydq",
  ]},
  Speedium => { addresses => [
    "f1luelfcqktlgpw55cqsqieyilusqrayzxbctsqfa",
    "f3qfjtedox66c3x6dgqt45e364ht3ydcfymcx6xucoy34s22y6jttatbcatsuppesupanb5476npnt5tdasrqa",
    "f3s5qozdyocudxtt2ekvcbu34dj2ubt3b4fqdzjqla7gj7442xckmlntcsmzibru6zkpiqlwflgn554cdhw6va",
    "f3sbunigqcrdk2xkxcgvrm72wt73fb4km2kykuy3xvb5gexf66kryjoweno2zlz6kyjuzcket72xd2s7b3zhoq",
    "f3shjxrnqvzn6x2dxojjpfplq5hhxww4qdtnfm42wosayutnjhtahpeu3qjcidz6y7kmqjrw6uuahhiua4vqyq",
    "f3tc2ebpyy7uojoewaxjec3hmsh4dmrdxdgmeyemnzr4hvwr7a7x5nvr5ykbjmsfaclychm5dha53inaiyq3da",
    "f3tf2q7aimiuezmlfe2obixtyjty3atxbwdz3vaqiekx3j3styocv6zgebkwdycpk2j5bvzstmosuqziezej2a",
  ]},
  "Starry sky in Yunnan" => { addresses => [
    "f3qqttrr5b6df5kjrtt6fwy3bafthqboe3aeghhqas2qiwavlxjlbdmo5xsai4s5hnam53pdiq55wsvkf52s6q",
  ]},
  "Wang Liang" => { addresses => [
    "f3sar6tat2mhqadlqjjxewmpqya3whpkzuqaevpo5vwfvdhnm3uztd7p67d6ozpsfkiwvrvqrhp3zu6psi23ma",
    "f3vsg65k7fs365uueavmfnwcd2wzxq62vdhh7ydgws6oi2xliwjenyp75cg4ynxjez3i2migafpgv26iyee74q",
  ]},
  liyiping => { addresses => [
    "f1jy3blozja7edk7firimlylpxpqkqzrx34grkcgy",
  ]},
  s0nik42 => { addresses => [
    "f3waqhsynu4a3w2a4mcwfaprd4fu6rgd3uuvt7mwj5v24br7u6vcb37xbvgbdgekaav3vcc3m6spz6etuh4aja",
  ]},
  "上海小工蚁电子商务股份有限公司" => { addresses => [
    "f1xl5b4vmjnikoku4j6brt33yoyqqepmqt7aowzny",
  ]},
  "北京中电博顺智能设备技术有限公司" => { addresses => [
    "f1uhubdateuntjmpje6yg4dwy6p5hjwxewuddueiy",
  ]},
  "安徽六六六科技有限公司" => { addresses => [
    "f14ew4fybr3grirbaii2yj4mmfo77kpayu74xegbi",
  ]},
  "杭州讯酷科技有限公司" => { addresses => [
    "f1wninfaoogdihtbqntlhjanb3z5bswqvjcestuuq",
  ]},
};

my $addr_to_client = {};
for my $c ( keys %$clients ) {
  $addr_to_client->{$_} = $c for @{ $clients->{$c}{addresses} || [] }
}

###
#
# Tabulate deals
#
my $resolved_addrs;
for my $deal ( values %$filp_deals ) {

  my $miner = $deal->{Proposal}{Provider};
  my $client_actor = $deal->{Proposal}{Client};
  my $dealsize = $deal->{Proposal}{PieceSize} / ( 1024 * 1024 * 1024 );

  my $addr = $resolved_addrs->{$client_actor} ||= do {
    decode_json( $ua->post($api_url, {
      headers => { 'content-type' => 'application/json' },
      content => encode_json({
        jsonrpc => '2.0',
        id => 1,
        method => 'Filecoin.StateAccountKey',
        params => [ $client_actor, undef ],
      }),
    })->{content} )->{result}
  };

  my $client = $addr_to_client->{$addr} ||= 'NEW/UNKNOWN';

  $clients->{$client}{filp_gb_stored_with_miner}{$miner} += $dealsize;
  $clients->{$client}{filp_gb_stored_total} += $dealsize;

  $miners->{$miner}{filp_gb_stored_for_address}{$addr} += $dealsize;
  $miners->{$miner}{filp_gb_stored_for_client}{$client} += $dealsize;
  $miners->{$miner}{filp_gb_stored_total} += $dealsize;
}

###
#
# Write out client stats
#
my $all_lines = [ [ "Client","Tot Fil+ GB" ] ];
my $orig_table_width = my $table_width = scalar @{ $all_lines->[0] };

for my $c ( sort { $clients->{$b}{filp_gb_stored_total} <=> $clients->{$a}{filp_gb_stored_total} or $a cmp $b } keys %$clients ) {
  my @lead = ( $c, sprintf( "%0.2f", $clients->{$c}{filp_gb_stored_total} ) );

  my $tot_pct = $clients->{$c}{filp_gb_stored_total} / 100;
  my $providers = $clients->{$c}{filp_gb_stored_with_miner};

  my @miner_line;
  my @pct_line;
  for my $miner ( sort { $providers->{$b} <=> $providers->{$a} or $a cmp $b } keys %$providers ) {
    push @miner_line, $miner;
    push @pct_line, sprintf( "%s / %0.2f%%", $miners->{$miner}{owner}||"Unknown", $providers->{$miner} / $tot_pct )
  }

  push @$all_lines, [], [ @lead, @miner_line ], [ @lead, @pct_line ];
  $table_width = $orig_table_width+@miner_line if $table_width < $orig_table_width+@miner_line;
}

$client_stats->print( join ',',
  @$_,
  ('') x ($table_width - @$_ ),
) && $client_stats->print( "\n" )
  for @$all_lines
;


###
#
# cut off miners with less than 1TiB of filp data, grab power ratio
#
$miners->{$_}{filp_gb_stored_total} < 1024 && delete $miners->{$_} for keys %$miners;

# get current miner filp ratio
for my $minerid ( keys %$miners ) {

  my $pow = decode_json( $ua->post($api_url, {
    headers => { 'content-type' => 'application/json' },
    content => encode_json({
      jsonrpc => '2.0',
      id => 1,
      method => 'Filecoin.StateMinerPower',
      params => [ $minerid, undef ],
    }),
  })->{content})->{result}{MinerPower};

  $miners->{$minerid}{filp_power_pct} = sprintf( '%0.2f', ( ( $pow->{QualityAdjPower} - $pow->{RawBytePower} ) / ( 0.09 * $pow->{RawBytePower} ) ) )
}



###
#
# Write out miner stats
#
$all_lines = [ [ 'MinerID', 'Owner', 'Tot Fil+ GB', '% of total bytes as Fil+' ] ];
$table_width = scalar @{ $all_lines->[0] };

for my $m ( sort { $miners->{$b}{filp_gb_stored_total} <=> $miners->{$a}{filp_gb_stored_total} or $a cmp $b } keys %$miners ) {
  push @$all_lines, [ $m, $miners->{$m}{owner}, sprintf( "%0.2f", $miners->{$m}{filp_gb_stored_total} ), $miners->{$m}{filp_power_pct} ];

  my $tot_pct = $miners->{$m}{filp_gb_stored_total} / 100;
  my $cls = $miners->{$m}{filp_gb_stored_for_client};

  for my $cl ( sort { $cls->{$b} <=> $cls->{$a} or $a cmp $b } keys %$cls ) {
    push @{$all_lines->[-1]}, sprintf( "%s / %0.2f%%", $cl||"Unknown", $cls->{$cl} / $tot_pct )
  }

  $table_width = @{$all_lines->[-1]} if $table_width < @{$all_lines->[-1]};
}

$miner_stats->print( join ',',
  @$_,
  ('') x ($table_width - @$_ ),
) && $miner_stats->print( "\n" )
  for @$all_lines
;
