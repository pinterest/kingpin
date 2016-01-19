#!/usr/bin/python
#
# Copyright 2016 Pinterest, Inc
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Test cases for FileWatch on config and serverset type."""

import os
import tempfile
import time

from unittest import TestCase

from kingpin.kazoo_utils.file_watch import FileWatch


class FileWatchTestCase(TestCase):

    def test_add_config_monitor(self):
        """
        When adding a monitor, the callback should be invoked.
        """
        fd, tmp_file = tempfile.mkstemp()

        test_data = [0, None, None]

        def on_change(value, stat):
            test_data[0] += 1
            test_data[1] = value
            test_data[2] = stat.version

        watch = FileWatch(polling=False)
        watch._clear_all_watches()
        self._validate_empty_internal_map(watch)
        watch.add_watch(tmp_file, on_change)
        self._validate_internal_map(watch, tmp_file, 'config', 1)
        self.assertEqual(test_data[0], 1)
        self.assertEqual(test_data[1], "")
        self.assertEqual(test_data[2], os.path.getmtime(tmp_file))
        del watch
        os.remove(tmp_file)

    def test_add_serverset_monitor(self):
        """
        When adding a monitor, the callback should be invoked.
        """
        fd, tmp_file = tempfile.mkstemp()

        test_data = [0, None]

        def on_change(children):
            test_data[0] += 1
            test_data[1] = children

        watch = FileWatch(polling=False)
        watch._clear_all_watches()
        self._validate_empty_internal_map(watch)
        watch.add_watch(tmp_file, on_change, watch_type='serverset')
        self._validate_internal_map(watch, tmp_file, 'serverset', 1)
        self.assertEqual(test_data[0], 1)
        self.assertEqual(test_data[1], [])
        del watch
        os.remove(tmp_file)

    def test_config_change(self):
        """
        When monitored file is changed, the callback should be invoked.
        """
        fd, tmp_file = tempfile.mkstemp()

        test_data = [0, None, None]

        def on_change(value, stat):
            test_data[0] += 1
            test_data[1] = value
            test_data[2] = stat.version

        new_content = "hello"
        watch = FileWatch(polling=False)
        watch._clear_all_watches()
        self._validate_empty_internal_map(watch)
        watch.add_watch(tmp_file, on_change)
        self._validate_internal_map(watch, tmp_file, 'config', 1)
        time.sleep(1)
        f = open(tmp_file, 'w')
        f.write(new_content)
        f.close()
        watch._check_file_updates()
        self._validate_internal_map(watch, tmp_file, 'config', 1)
        self.assertEqual(test_data[0], 2)
        self.assertEqual(test_data[1], new_content)
        self.assertEqual(test_data[2], os.path.getmtime(tmp_file))

        # Rerun check_updates won't invoke callback
        watch._check_file_updates()
        self.assertEqual(test_data[0], 2)
        self.assertEqual(test_data[1], new_content)
        self.assertEqual(test_data[2], os.path.getmtime(tmp_file))

        del watch
        os.remove(tmp_file)

    def test_serverset_change(self):
        """
        When monitored serverset is changed, the callback should be invoked.
        """
        fd, tmp_file = tempfile.mkstemp()

        test_data = [0, None]

        def on_change(value):
            test_data[0] += 1
            test_data[1] = value

        new_content = "hello"
        watch = FileWatch(polling=False)
        watch._clear_all_watches()
        self._validate_empty_internal_map(watch)
        watch.add_watch(tmp_file, on_change, watch_type='serverset')
        self._validate_internal_map(watch, tmp_file, 'serverset', 1)
        time.sleep(1)
        f = open(tmp_file, 'w')
        f.write(new_content)
        f.close()
        watch._check_file_updates()
        self._validate_internal_map(watch, tmp_file, 'serverset', 1)
        self.assertEqual(test_data[0], 2)
        self.assertEqual(test_data[1], [new_content])

        # Rerun check_updates won't invoke callback
        watch._check_file_updates()
        self.assertEqual(test_data[0], 2)
        self.assertEqual(test_data[1], [new_content])

        del watch
        os.remove(tmp_file)

    def test_config_content_not_change(self):
        """
        When monitored config has mtime changed but not the content,
        callback should not be triggered.
        """
        fd, tmp_file = tempfile.mkstemp()

        test_data = [0, None, None]

        def on_change(value, stat):
            test_data[0] += 1
            test_data[1] = value
            test_data[2] = stat.version

        new_content = "hello"
        watch = FileWatch(polling=False)
        watch._clear_all_watches()
        self._validate_empty_internal_map(watch)
        watch.add_watch(tmp_file, on_change)
        self._validate_internal_map(watch, tmp_file, 'config', 1)
        time.sleep(1)
        f = open(tmp_file, 'w')
        f.write(new_content)
        f.close()
        watch._check_file_updates()
        self._validate_internal_map(watch, tmp_file, 'config', 1)
        old_update_time = os.path.getmtime(tmp_file)
        self.assertEqual(test_data[0], 2)
        self.assertEqual(test_data[1], new_content)
        self.assertEqual(test_data[2], old_update_time)

        time.sleep(1)
        f = open(tmp_file, 'w')
        # Writing the same content
        f.write(new_content)
        f.close()
        new_update_time = os.path.getmtime(tmp_file)
        self.assertNotEqual(old_update_time, new_update_time)
        # Rerun check_updates won't invoke callback
        watch._check_file_updates()
        self._validate_internal_map(watch, tmp_file, 'config', 1)
        self.assertEqual(test_data[0], 2)
        self.assertEqual(test_data[1], new_content)
        self.assertEqual(test_data[2], old_update_time)

        del watch
        os.remove(tmp_file)

    def test_serverset_content_not_change(self):
        """
        When monitored serverset has mtime changed but not the content,
        callback should not be triggered.
        """
        fd, tmp_file = tempfile.mkstemp()

        test_data = [0, None]

        def on_change(value):
            test_data[0] += 1
            test_data[1] = value

        new_content = "hello"
        watch = FileWatch(polling=False)
        watch._clear_all_watches()
        self._validate_empty_internal_map(watch)
        watch.add_watch(tmp_file, on_change, watch_type='serverset')
        self._validate_internal_map(watch, tmp_file, 'serverset', 1)
        time.sleep(1)
        f = open(tmp_file, 'w')
        f.write(new_content)
        f.close()
        watch._check_file_updates()
        self._validate_internal_map(watch, tmp_file, 'serverset', 1)
        old_update_time = os.path.getmtime(tmp_file)
        self.assertEqual(test_data[0], 2)
        self.assertEqual(test_data[1], [new_content])

        time.sleep(1)
        f = open(tmp_file, 'w')
        # Writing the same content
        f.write(new_content)
        f.close()
        new_update_time = os.path.getmtime(tmp_file)
        self.assertNotEqual(old_update_time, new_update_time)
        # Rerun check_updates won't invoke callback
        watch._check_file_updates()
        self._validate_internal_map(watch, tmp_file, 'serverset', 1)
        self.assertEqual(test_data[0], 2)
        self.assertEqual(test_data[1], [new_content])

        del watch
        os.remove(tmp_file)

    def test_multiple_config_changes(self):
        """
        When monitored file is changed multiple times, the callback
        should be invoked every time.
        """
        fd, tmp_file = tempfile.mkstemp()

        test_data = [0, None, None]

        def on_change(value, stat):
            test_data[0] += 1
            test_data[1] = value
            test_data[2] = stat.version

        new_content = "hello"
        watch = FileWatch(polling=False)
        watch._clear_all_watches()
        self._validate_empty_internal_map(watch)
        watch.add_watch(tmp_file, on_change)
        self._validate_internal_map(watch, tmp_file, 'config', 1)
        times = 3
        expected_content = ""
        for i in range(times):
            time.sleep(1)
            new_line = new_content + str(i) + '\n'
            f = open(tmp_file, 'a')
            f.write(new_line)
            f.close()
            watch._check_file_updates()
            self._validate_internal_map(watch, tmp_file, 'config', 1)
            expected_content += new_line
            self.assertEqual(test_data[0], i+2)
            self.assertEqual(test_data[1], expected_content)
            self.assertEqual(test_data[2], os.path.getmtime(tmp_file))
        del watch
        os.remove(tmp_file)

    def test_multiple_serverset_changes(self):
        """
        When monitored serverset is changed multiple times, the callback
        should be invoked every time.
        """
        fd, tmp_file = tempfile.mkstemp()

        test_data = [0, None]

        def on_change(value):
            test_data[0] += 1
            test_data[1] = value

        new_content = "hello"
        watch = FileWatch(polling=False)
        watch._clear_all_watches()
        self._validate_empty_internal_map(watch)
        watch.add_watch(tmp_file, on_change, watch_type='serverset')
        self._validate_internal_map(watch, tmp_file, 'serverset', 1)
        times = 3
        expected_content = []
        isFirst = True
        for i in range(times):
            time.sleep(1)
            new_line = new_content + str(i)
            f = open(tmp_file, 'a')
            if isFirst:
                isFirst = False
            else:
                f.write("\n")
            f.write(new_line)
            f.close()
            watch._check_file_updates()
            self._validate_internal_map(watch, tmp_file, 'serverset', 1)
            expected_content.append(new_line)
            self.assertEqual(test_data[0], i+2)
            self.assertEqual(test_data[1], expected_content)
        del watch
        os.remove(tmp_file)

    def test_multiple_watchers_on_single_config(self):
        """
        When one config file has multiple watchers, all will be invoked
        when the config changes.
        """
        fd, tmp_file = tempfile.mkstemp()

        test_data1 = [0, None, None]

        def on_change1(value, stat):
            test_data1[0] += 1
            test_data1[1] = value
            test_data1[2] = stat.version

        test_data2 = [0, None, None]

        def on_change2(value, stat):
            test_data2[0] += 1
            test_data2[1] = value
            test_data2[2] = stat.version

        new_content = "hello"
        watch = FileWatch(polling=False)
        watch._clear_all_watches()
        self._validate_empty_internal_map(watch)
        watch.add_watch(tmp_file, on_change1)
        self._validate_internal_map(watch, tmp_file, 'config', 1)
        watch.add_watch(tmp_file, on_change2)
        self._validate_internal_map(watch, tmp_file, 'config', 2)
        time.sleep(1)
        f = open(tmp_file, 'w')
        f.write(new_content)
        f.close()
        watch._check_file_updates()
        self._validate_internal_map(watch, tmp_file, 'config', 2)
        self.assertEqual(test_data1[0], 2)
        self.assertEqual(test_data1[1], new_content)
        self.assertEqual(test_data1[2], os.path.getmtime(tmp_file))
        self.assertEqual(test_data2[0], 2)
        self.assertEqual(test_data2[1], new_content)
        self.assertEqual(test_data2[2], os.path.getmtime(tmp_file))
        del watch
        os.remove(tmp_file)

    def test_multiple_watchers_on_single_serverset(self):
        """
        When one serverset file has multiple watchers, all will be invoked
        when the serverset changes.
        """
        fd, tmp_file = tempfile.mkstemp()

        test_data1 = [0, None]

        def on_change1(value):
            test_data1[0] += 1
            test_data1[1] = value

        test_data2 = [0, None]

        def on_change2(value):
            test_data2[0] += 1
            test_data2[1] = value

        new_content = "hello\nworld"
        watch = FileWatch(polling=False)
        watch._clear_all_watches()
        self._validate_empty_internal_map(watch)
        watch.add_watch(tmp_file, on_change1, watch_type='serverset')
        self._validate_internal_map(watch, tmp_file, 'serverset', 1)
        watch.add_watch(tmp_file, on_change2, watch_type='serverset')
        self._validate_internal_map(watch, tmp_file, 'serverset', 2)
        time.sleep(1)
        f = open(tmp_file, 'w')
        f.write(new_content)
        f.close()
        expected_serverset = new_content.split('\n')
        watch._check_file_updates()
        self._validate_internal_map(watch, tmp_file, 'serverset', 2)
        self.assertEqual(test_data1[0], 2)
        self.assertEqual(test_data1[1], expected_serverset)
        self.assertEqual(test_data2[0], 2)
        self.assertEqual(test_data2[1], expected_serverset)
        del watch
        os.remove(tmp_file)

    def test_unrecognized_watch_type(self):
        fd, tmp_file = tempfile.mkstemp()

        watch = FileWatch(polling=False)
        watch._clear_all_watches()
        self._validate_empty_internal_map(watch)

        def on_change(value, stat):
            pass
        self.assertRaises(
            Exception,
            watch.add_watch,
            tmp_file,
            on_change,
            "__not_exist"
        )
        self._validate_empty_internal_map(watch)
        del watch

    def test_exception_on_config_callback(self):
        """
        When config's callback has exception, It should not prevent other
        watchers from getting notified.
        """
        fd, tmp_file = tempfile.mkstemp()

        def on_change1(value, stat):
            raise Exception

        test_data2 = [0, None, None]

        def on_change2(value, stat):
            test_data2[0] += 1
            test_data2[1] = value
            test_data2[2] = stat.version

        new_content = "hello"
        watch = FileWatch(polling=False)
        watch._clear_all_watches()
        self._validate_empty_internal_map(watch)
        watch.add_watch(tmp_file, on_change1)
        self._validate_internal_map(watch, tmp_file, 'config', 1)
        watch.add_watch(tmp_file, on_change2)
        self._validate_internal_map(watch, tmp_file, 'config', 2)
        time.sleep(1)
        f = open(tmp_file, 'w')
        f.write(new_content)
        f.close()
        watch._check_file_updates()
        self._validate_internal_map(watch, tmp_file, 'config', 2)
        self.assertEqual(test_data2[0], 2)
        self.assertEqual(test_data2[1], new_content)
        self.assertEqual(test_data2[2], os.path.getmtime(tmp_file))
        del watch
        os.remove(tmp_file)

    def test_exception_on_serverset_callback(self):
        """
        When serverset's callback has exception, It should not prevent other
        watchers from getting notified.
        """
        fd, tmp_file = tempfile.mkstemp()

        def on_change1(value):
            raise Exception

        test_data2 = [0, None]

        def on_change2(value):
            test_data2[0] += 1
            test_data2[1] = value

        new_content = "hello\nworld"
        watch = FileWatch(polling=False)
        watch._clear_all_watches()
        self._validate_empty_internal_map(watch)
        watch.add_watch(tmp_file, on_change1, watch_type='serverset')
        self._validate_internal_map(watch, tmp_file, 'serverset', 1)
        watch.add_watch(tmp_file, on_change2, watch_type='serverset')
        self._validate_internal_map(watch, tmp_file, 'serverset', 2)
        time.sleep(1)
        f = open(tmp_file, 'w')
        f.write(new_content)
        f.close()
        expected_serverset = new_content.split('\n')
        watch._check_file_updates()
        self._validate_internal_map(watch, tmp_file, 'serverset', 2)
        self.assertEqual(test_data2[0], 2)
        self.assertEqual(test_data2[1], expected_serverset)
        del watch
        os.remove(tmp_file)

    def test_nonexistent_config(self):
        def on_change(value, stat):
            pass

        watch = FileWatch(polling=False)
        watch._clear_all_watches()
        self._validate_empty_internal_map(watch)
        self.assertRaises(
            OSError,
            watch.add_watch,
            "__nonexistent__",
            on_change
        )
        self._validate_empty_internal_map(watch)
        del watch

    def test_nonexistent_serverest(self):
        def on_change(value):
            pass

        watch = FileWatch(polling=False)
        watch._clear_all_watches()
        self._validate_empty_internal_map(watch)
        self.assertRaises(
            OSError,
            watch.add_watch,
            "__nonexistent__",
            on_change,
            'serverset'
        )
        self._validate_empty_internal_map(watch)
        del watch

    def test_null_callback_config(self):
        watch = FileWatch(polling=False)
        watch._clear_all_watches()
        self._validate_empty_internal_map(watch)
        self.assertRaises(
            AssertionError,
            watch.add_watch,
            "__nonexistent__",
            None
        )
        self._validate_empty_internal_map(watch)
        del watch

    def test_null_callback_serverset(self):
        watch = FileWatch(polling=False)
        watch._clear_all_watches()
        self._validate_empty_internal_map(watch)
        self.assertRaises(
            AssertionError,
            watch.add_watch,
            "__nonexistent__",
            None,
            'serverset'
        )
        self._validate_empty_internal_map(watch)
        del watch

    def _validate_empty_internal_map(self, watch):
        self.assertEqual(watch._watched_file_map, {})

    def _validate_internal_map(self, watch, file_path, watch_type, watcher_count):
        key = (file_path, watch_type)
        watch_info = watch._watched_file_map[key]
        update_time = os.path.getmtime(file_path)
        with open(file_path) as f:
            hash = watch._compute_md5_hash(f.read())
        self.assertEqual(watch_info[0], update_time)
        self.assertEqual(watch_info[1], hash)
        self.assertEqual(len(watch_info[2]), watcher_count)
