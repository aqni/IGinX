#!/bin/bash
#
# IGinX - the polystore system with high performance
# Copyright (C) Tsinghua University
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#
 

set -e

sh -c "mkdir -p test/src/test/resources/fileReadAndWrite/byteStream"

sh -c "mkdir -p test/src/test/resources/fileReadAndWrite/csv"

bash -c "mkdir downloads"

downloadURL="https://raw.githubusercontent.com/IGinX-THU/IGinX-resources/main/iginx-python-example/largeImg/large_img.jpg"

bash -c "curl -LJ $downloadURL -o downloads/large_img.jpg"

COMMAND='clear data;'

COMMAND+='insert into test(key, s1) values (0, 0), (1, 1), (2, 2), (3, 3), (4, 4);'

COMMAND+='insert into test(key, s2) values (0, 0.5), (1, 1.5), (2, 2.5), (3, 3.5), (4, 4.5);'

COMMAND+='insert into test(key, s3) values (0, true), (1, false), (2, true), (3, false), (4, true);'

COMMAND+='insert into test(key, s4) values (0, "'"aaa"'"), (1, "'"bbb"'"), (2, "'"ccc"'"), (3, "'"ddd"'"), (4, "'"eee"'");'

COMMAND+='select * from test into outfile "'"test/src/test/resources/fileReadAndWrite/byteStream"'" as stream;'

COMMAND+='select * from test into outfile "'"test/src/test/resources/fileReadAndWrite/csv/test.csv"'" as csv;'

bash -c "sleep 10"

bash -c "client/target/iginx-client-$2/sbin/start_cli.bat -e '$COMMAND'"

mkdir -p "test/src/test/resources/fileReadAndWrite/byteDummy"

# add exported dir as dummy fs storge, then test export
bash -c "cp -r test/src/test/resources/fileReadAndWrite/byteStream/* test/src/test/resources/fileReadAndWrite/byteDummy"
# add extension to filename
for file in test/src/test/resources/fileReadAndWrite/byteDummy/*; do
    mv "$file" "${file}.ext"
done

bash -c "client/target/iginx-client-$2/sbin/start_cli.bat -e 'ADD STORAGEENGINE ("'"127.0.0.1"'", 6670, "'"filesystem"'", "'"dummy_dir:test/src/test/resources/fileReadAndWrite/byteDummy,iginx_port:6888,has_data:true,is_read_only:true"'");show columns byteDummy.*;'"

bash -c "client/target/iginx-client-$2/sbin/start_cli.bat -e 'select * from byteDummy into outfile "'"test/src/test/resources/fileReadAndWrite/byteStreamExport"'" as stream;'"

db_name=$1

# 只测FileSystem和Parquet
if [[ "$db_name" != "FileStore" ]] && [[ "$db_name" != "FileSystem" ]] && [[ "$db_name" != "Parquet" ]]; then
  exit 0
fi

# 将 downloads/large_img.jpg 的数据加载到IGinX数据库中
bash -c "mvn test -q -Dtest=FileLoaderTest#loadLargeImage -Dsurefire.failIfNoSpecifiedTests=false -P-format"

OUTFILE_COMMAND='select count(*) from downloads;select large_img_jpg from downloads into outfile "'"test/src/test/resources/fileReadAndWrite/img_outfile"'" as stream;'

bash -c "client/target/iginx-client-$2/sbin/start_cli.bat -e '$OUTFILE_COMMAND'"