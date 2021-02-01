import re

"""
This file is part of Splice Machine.
Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
GNU Affero General Public License as published by the Free Software Foundation, either
version 3, or (at your option) any later version.
Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
See the GNU Affero General Public License for more details.
You should have received a copy of the GNU Affero General Public License along with Splice Machine.
If not, see <http://www.gnu.org/licenses/>.

Unless required by applicable law or agreed to in writing, software distributed
under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
CONDITIONS OF ANY KIND, either express or implied. See the License for the
specific language governing permissions and limitations under the License.

All such Splice Machine modifications are Copyright 2012 - 2020 Splice Machine, Inc.,
and are licensed to you under the GNU Affero General Public License.
"""



# keywords that trigger double quotation with "<identifier>"
# otherwise splice will interpret them as SQL keywords
RESERVED_WORDS = {'activate', 'disallow', 'locale', 'result', 'add', 'disconnect', 'localtime', 'result_set_locator', 'size',
                  'after', 'distinct', 'localtimestamp', 'return', 'alias', 'do', 'locator', 'returns', 'all', 'double',
                  'locators', 'revoke', 'allocate', 'drop', 'lock', 'right', 'allow', 'dssize', 'lockmax', 'rollback',
                  'alter', 'dynamic', 'locksize', 'routine', 'and', 'each', 'long', 'row', 'any', 'editproc', 'loop',
                  'row_number', 'as', 'else', 'maintained', 'rownumber', 'asensitive', 'elseif', 'materialized', 'rows',
                  'associate', 'enable', 'maxvalue', 'rowset', 'asutime', 'encoding', 'microsecond', 'rrn', 'at',
                  'encryption', 'microseconds', 'run', 'attributes', 'end', 'minute', 'savepoint', 'audit', 'end-exec',
                  'minutes', 'schema', 'authorization', 'ending', 'minvalue', 'scratchpad', 'aux', 'erase', 'mode',
                  'scroll', 'auxiliary', 'escape', 'modifies', 'search', 'before', 'every', 'month', 'second', 'begin',
                  'except', 'months', 'seconds', 'between', 'exception', 'new', 'secqty', 'binary', 'excluding',
                  'new_table', 'security', 'bufferpool', 'exclusive', 'nextval', 'select', 'by', 'execute', 'no',
                  'sensitive', 'cache', 'exists', 'nocache', 'sequence', 'call', 'exit', 'nocycle', 'session', 'called',
                  'explain', 'nodename', 'session_user', 'capture', 'external', 'nodenumber', 'set', 'cardinality',
                  'extract', 'nomaxvalue', 'signal', 'cascaded', 'fenced', 'nominvalue', 'simple', 'case', 'fetch',
                  'none', 'some', 'cast', 'fieldproc', 'noorder', 'source', 'ccsid', 'file', 'normalized', 'specific',
                  'char', 'final', 'not', 'sql', 'character', 'for', 'null', 'sqlid', 'check', 'foreign', 'nulls',
                  'stacked', 'close', 'free', 'numparts', 'standard', 'cluster', 'from', 'obid', 'start', 'collection',
                  'full', 'of', 'starting', 'collid', 'function', 'old', 'statement', 'column', 'general', 'old_table',
                  'static', 'comment', 'generated', 'on', 'stay', 'commit', 'get', 'open', 'stogroup', 'concat',
                  'global', 'optimization', 'stores', 'condition', 'go', 'optimize', 'style', 'connect', 'goto',
                  'option', 'substring', 'connection', 'grant', 'or', 'summary', 'constraint', 'graphic', 'order',
                  'synonym', 'contains', 'group', 'out', 'sysfun', 'continue', 'handler', 'outer', 'sysibm', 'count',
                  'hash', 'over', 'sysproc', 'count_big', 'hashed_value', 'overriding', 'system', 'create', 'having',
                  'package', 'system_user', 'cross', 'hint', 'padded', 'table', 'current', 'hold', 'pagesize',
                  'tablespace', 'current_date', 'hour', 'parameter', 'then', 'current_lc_ctype', 'hours', 'part',
                  'time', 'current_path', 'identity', 'partition', 'timestamp', 'current_schema', 'if', 'partitioned',
                  'to', 'current_server', 'immediate', 'partitioning', 'transaction', 'current_time', 'in',
                  'partitions', 'trigger', 'current_timestamp', 'including', 'password', 'trim', 'current_timezone',
                  'inclusive', 'path', 'type', 'current_user', 'increment', 'piecesize', 'undo', 'cursor', 'index',
                  'plan', 'union', 'cycle', 'indicator', 'position', 'unique', 'data', 'inherit', 'precision', 'until',
                  'database', 'inner', 'prepare', 'update', 'datapartitionname', 'inout', 'prevval', 'usage',
                  'datapartitionnum', 'insensitive', 'primary', 'user', 'second', 'date', 'insert', 'priqty', 'using',
                  'day', 'integrity', 'privileges', 'validproc', 'days', 'intersect', 'procedure', 'value',
                  'db2general', 'into', 'program', 'values', 'db2genrl', 'is', 'psid', 'variable', 'db2sql', 'isobid',
                  'query', 'variant', 'dbinfo', 'isolation', 'queryno', 'vcat', 'dbpartitionname', 'iterate', 'range',
                  'version', 'dbpartitionnum', 'jar', 'rank', 'view', 'deallocate', 'java', 'read', 'volatile',
                  'declare', 'join', 'reads', 'volumes', 'default', 'key', 'recovery', 'when', 'defaults', 'label',
                  'references', 'whenever', 'definition', 'language', 'referencing', 'where', 'delete', 'lateral',
                  'refresh', 'while', 'dense_rank', 'lc_ctype', 'release', 'with', 'denserank', 'leave', 'rename',
                  'without', 'describe', 'left', 'repeat', 'wlm', 'descriptor', 'like', 'reset', 'write',
                  'deterministic', 'linktype', 'resignal', 'xmlelement', 'diagnostics', 'local', 'restart', 'year',
                  'disable', 'localdate', 'restrict', 'years', '', 'abs', 'grouping', 'regr_intercept', 'are', 'int',
                  'regr_r2', 'array', 'integer', 'regr_slope', 'asymmetric', 'intersection', 'regr_sxx', 'atomic',
                  'interval', 'regr_sxy', 'avg', 'large', 'regr_syy', 'bigint', 'first', 'last', 'leading', 'rollup',
                  'blob', 'ln', 'scope', 'boolean', 'lower', 'similar', 'both', 'match', 'smallint', 'ceil', 'max',
                  'specifictype', 'ceiling', 'member', 'sqlexception', 'char_length', 'merge', 'sqlstate',
                  'character_length', 'method', 'sqlwarning', 'clob', 'min', 'sqrt', 'coalesce', 'mod', 'stddev_pop',
                  'collate', 'module', 'stddev_samp', 'collect', 'multiset', 'submultiset', 'convert', 'national',
                  'sum', 'corr', 'natural', 'symmetric', 'corresponding', 'nchar', 'tablesample', 'covar_pop', 'nclob',
                  'timezone_hour', 'covar_samp', 'normalize', 'timezone_minute', 'cube', 'nullif', 'trailing',
                  'cume_dist', 'numeric', 'translate', 'current_default_transform_group', 'octet_length', 'translation',
                  'current_role', 'only', 'treat', 'current_transform_group_for_type', 'overlaps', 'true', 'dec',
                  'overlay', 'uescape', 'decimal', 'percent_rank', 'unknown', 'deref', 'percentile_cont', 'unnest',
                  'element', 'percentile_disc', 'upper', 'exec', 'power', 'var_pop', 'exp', 'real', 'var_samp', 'false',
                  'recursive', 'varchar', 'filter', 'ref', 'varying', 'float', 'regr_avgx', 'width_bucket', 'floor',
                  'regr_avgy', 'window', 'fusion', 'regr_count', 'within', 'asc'}

# case insensitive reserved words regular expression
# for efficient matching (w/o iteration)
# we need [1:] to remove first pipe from string
# or it will match everything
RESERVED_WORDS_REGEX = re.compile('|'.join(RESERVED_WORDS)[1:], re.IGNORECASE)
