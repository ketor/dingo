/*
 * Copyright 2021 DataCanvas
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.dingodb.mpu.storage.rocks;

import io.dingodb.common.codec.PrimitiveCodec;
import io.dingodb.common.concurrent.Executors;
import io.dingodb.common.concurrent.LinkedRunner;
import io.dingodb.common.util.FileUtils;
import io.dingodb.common.util.Optional;
import io.dingodb.mpu.api.StorageApi;
import io.dingodb.mpu.core.CoreMeta;
import io.dingodb.mpu.instruction.Instruction;
import io.dingodb.mpu.storage.Storage;
import io.dingodb.net.service.FileTransferService;
import lombok.extern.slf4j.Slf4j;
import org.rocksdb.*;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.Comparator;

import static io.dingodb.common.codec.PrimitiveCodec.encodeLong;
import static io.dingodb.mpu.Constant.API;
import static io.dingodb.mpu.Constant.CF_DEFAULT;
import static io.dingodb.mpu.Constant.CF_META;
import static io.dingodb.mpu.Constant.CLOCK_K;
import static java.util.Collections.singletonList;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.rocksdb.SizeApproximationFlag.INCLUDE_FILES;
import static org.rocksdb.SizeApproximationFlag.INCLUDE_MEMTABLES;

@Slf4j
public class RocksStorage implements Storage {

    static {
        RocksDB.loadLibrary();
    }

    public final CoreMeta coreMeta;

    public final Path path;
    public final Path instructionPath;
    public final Path dbPath;
    public final Path backupPath;
    public final Path checkpointPath;
    public final Path dcfPath;
    public final Path mcfPath;
    public final Path icfPath;

    public final String dbRocksOptionsFile;
    public final String logRocksOptionsFile;
    public final int ttl;

    public final WriteOptions writeOptions;
    public final LinkedRunner runner;

    public BackupEngine backup;
    public Checkpoint checkpoint;
    public RocksDB instruction;
    public RocksDB db;

    private ColumnFamilyHandle dcfHandler;
    private ColumnFamilyHandle mcfHandler;
    private ColumnFamilyHandle icfHandler;

    private ColumnFamilyDescriptor dcfDesc;
    private ColumnFamilyDescriptor mcfDesc;
    private ColumnFamilyDescriptor icfDesc;

    private boolean destroy = false;
    private boolean use_checkpoint = true;
    private boolean disable_checkpoint_purge = false;

    private static final int MAX_BLOOM_HASH_NUM = 10;
    private static final int MAX_PREFIX_LENGTH = 4;

    public static final String LOCAL_CHECKPOINT_PREFIX = "local-";
    public static final String REMOTE_CHECKPOINT_PREFIX = "remote-";

    public RocksStorage(CoreMeta coreMeta, String path, final String dbRocksOptionsFile,
                        final String logRocksOptionsFile, final int ttl) throws Exception {
        this.coreMeta = coreMeta;
        this.runner = new LinkedRunner(coreMeta.label);
        this.path = Paths.get(path).toAbsolutePath();
        this.dbRocksOptionsFile = dbRocksOptionsFile;
        this.logRocksOptionsFile = logRocksOptionsFile;
        this.ttl = ttl;

        this.backupPath = this.path.resolve("backup");
        this.checkpointPath = this.path.resolve("checkpoint");

        this.dbPath = this.path.resolve("db");
        this.dcfPath = this.dbPath.resolve("data");
        this.mcfPath = this.dbPath.resolve("meta");

        this.instructionPath = this.path.resolve("instruction");
        this.icfPath = this.instructionPath.resolve("data");
        FileUtils.createDirectories(this.instructionPath);
        FileUtils.createDirectories(this.backupPath);
        FileUtils.createDirectories(this.checkpointPath);
        FileUtils.createDirectories(this.dbPath);
        this.instruction = createInstruction();
        log.info("Create {} instruction db.", coreMeta.label);
        this.db = createDB();
        this.writeOptions = new WriteOptions();
        log.info("Create {} db,  ttl: {}.", coreMeta.label, this.ttl);
        backup = BackupEngine.open(db.getEnv(), new BackupEngineOptions(backupPath.toString()));
        checkpoint = Checkpoint.create(db);
        log.info("Create rocks storage for {} success.", coreMeta.label);
    }

    private RocksDB createInstruction() throws RocksDBException {
        DBOptions options = new DBOptions();
        loadDBOptions(this.logRocksOptionsFile, options);

        options.setCreateIfMissing(true);
        options.setCreateMissingColumnFamilies(true);
        options.setListeners(Collections.singletonList(new Listener()));
        options.setWalDir(this.instructionPath.resolve("wal").toString());

        final ColumnFamilyOptions cfOption = new ColumnFamilyOptions();
        BlockBasedTableConfig tableConfig = new BlockBasedTableConfig();
        tableConfig.setBlockSize(128 * 1024);
        tableConfig.setBlockCacheSize(200 / 4 * 1024 * 1024 * 1024L);
        cfOption.setTableFormatConfig(tableConfig);
        cfOption.setArenaBlockSize(128 * 1024 * 1024);
        cfOption.setMinWriteBufferNumberToMerge(4);
        cfOption.setMaxWriteBufferNumber(5);
        cfOption.setMaxCompactionBytes(512 * 1024 * 1024);
        cfOption.setWriteBufferSize(1 * 1024 * 1024 * 1024);
        cfOption.useFixedLengthPrefixExtractor(8);
        cfOption.setMergeOperator(new StringAppendOperator());

        icfDesc = icfDesc(cfOption);
        List<ColumnFamilyDescriptor> cfs = new ArrayList<>();
        cfs.add(icfDesc);

        List<ColumnFamilyHandle> handles = new ArrayList<>();
        RocksDB instruction = RocksDB.open(options, this.instructionPath.toString(), cfs, handles);
        log.info("RocksStorage createInstruction, RocksDB open, path: {}, options file: {}, handles size: {}.",
            this.instructionPath, this.logRocksOptionsFile, handles.size());
        this.icfHandler = handles.get(0);
        assert (this.icfHandler != null);

        return instruction;
    }

    private RocksDB createDB() throws Exception {
        DBOptions options = new DBOptions();
        loadDBOptions(this.dbRocksOptionsFile, options);
        options.setCreateIfMissing(true);
        options.setCreateMissingColumnFamilies(true);
        options.setWalDir(this.dbPath.resolve("wal").toString());

        /**
         * configuration for performance.
         * 1. max_background_compaction
         * 2. max_background_flushes
         * 3. max_background_jobs
         * 4. bytes_per_sync: 1M
         * 5. db_write_buffer_size: 2G
         */
        options.setListeners(Collections.singletonList(new Listener()));

        List<ColumnFamilyDescriptor> cfs = Arrays.asList(
            dcfDesc = dcfDesc(),
            mcfDesc = mcfDesc()
        );

        RocksDB db;
        List<ColumnFamilyHandle> handles = new ArrayList<>(4);
        if (RocksUtils.ttlValid(this.ttl)) {
            List<Integer> ttlList = new ArrayList<>();
            ttlList.add(this.ttl);
            ttlList.add(0);
            db = TtlDB.open(options, this.dbPath.toString(), cfs, handles, ttlList, false, true);
            Executors.scheduleWithFixedDelayAsync("kv-compact", this::compact,  60 * 60, 60 * 60,
                SECONDS);
        } else {
            db = RocksDB.open(options, this.dbPath.toString(), cfs, handles);
        }
        log.info("RocksStorage createDB, RocksDB open, path: {}, options file: {}, handles size: {}, ttl: {}.",
            this.dbPath, this.dbRocksOptionsFile, handles.size(), this.ttl);
        this.dcfHandler = handles.get(0);
        this.mcfHandler = handles.get(1);
        return db;
    }

    private boolean loadDBOptions(final String optionsFile, DBOptions options) {
        try {
            if (optionsFile == null || !(new File(optionsFile)).exists()) {
                log.info("loadDBOptions, rocksdb options file not found: {}, use default options.", optionsFile);
                return false;
            }
            OptionsUtil.loadDBOptionsSimplyFromFile(new ConfigOptions(), optionsFile, options);
            return true;
        } catch (RocksDBException dbException) {
            log.warn("loadDBOptions, load {} exception, use default options.", optionsFile, dbException);
            return false;
        }
    }

    public void closeDB() {
        this.db.cancelAllBackgroundWork(true);
        this.dcfHandler.close();
        this.dcfHandler = null;
        this.mcfHandler.close();
        this.mcfHandler = null;
        this.db.close();
        this.db = null;
    }

    @Override
    public void destroy() {
        destroy = true;
        this.writeOptions.close();
        closeDB();
        this.icfHandler.close();
        this.icfHandler = null;
        this.instruction.close();
        this.instruction = null;
        this.backup.close();
        this.backup = null;
        this.checkpoint.close();
        this.checkpoint = null;
        /**
         * to avoid the file handle leak when drop table
         */
        // FileUtils.deleteIfExists(path);
    }

    @Override
    protected void finalize() throws Throwable {
        super.finalize();
        try {
            dcfDesc.getOptions().close();
            mcfDesc.getOptions().close();
            icfDesc.getOptions().close();
        } catch (Exception e) {
            log.error("Close {} cf options error.", coreMeta.label, e);
        }
    }

    @Override
    public CompletableFuture<Void> transferTo(CoreMeta meta) {
        log.info(String.format("RocksStorage::transferTo [%s][%s]", meta.label, meta.location.toString()));
        return Executors.submit("transfer-to-" + meta.label, () -> {
            backup();
            StorageApi storageApi = API.proxy(StorageApi.class, meta.location);
            String target = storageApi.transferBackup(meta.mpuId, meta.coreId);
            if (use_checkpoint) {
                this.disable_checkpoint_purge = true;
                String checkpoint_name = GetLatestCheckpointName(LOCAL_CHECKPOINT_PREFIX);
                FileTransferService.transferTo(meta.location, checkpointPath.resolve(checkpoint_name), Paths.get(target));
                this.disable_checkpoint_purge = false;
            } else {
                FileTransferService.transferTo(meta.location, backupPath, Paths.get(target));
            }
            storageApi.applyBackup(meta.mpuId, meta.coreId);
            return null;
        });
    }

    private void flushMeta() {
        try (FlushOptions flushOptions = new FlushOptions().setWaitForFlush(true)) {

            if (db != null && mcfHandler != null) {
                db.flush(flushOptions, mcfHandler);
            }
        } catch (RocksDBException e) {
            log.error("Flush instruction error.", e);
        }
    }

    private void flushInstruction() {
        try (FlushOptions flushOptions = new FlushOptions().setWaitForFlush(true)) {
            if (instruction != null) {
                instruction.flush(flushOptions);
            }
        } catch (RocksDBException e) {
            log.error("Flush instruction error.", e);
        }
    }

    /**
     * Create new RocksDB checkpoint in backup dir
     *
     * @throws RuntimeException
     */
    public void createNewCheckpoint() {
        if (destroy) {
            return;
        }
        try {
            if (checkpoint != null) {
                // use nanoTime as checkpoint_name
                String checkpoint_name = String.format("%s%d", LOCAL_CHECKPOINT_PREFIX, System.nanoTime());
                String checkpoint_dir_name = this.checkpointPath.resolve(checkpoint_name).toString();
                checkpoint.createCheckpoint(checkpoint_dir_name);
            }
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Get Latest Checkpoint Dir Name
     *
     * @param prefix Prefix of the checkpoint_name
     *              Use prefix to identify remote or local checkpoint.
     */
    public String GetLatestCheckpointName(String prefix){
        String latest_checkpoint_name = "";
        if (checkpoint != null) {
            File[] directories = new File(this.checkpointPath.toString()).listFiles(File::isDirectory);
            if (directories.length > 0) {
                for (File checkpoint_dir: directories) {
                    // dir name end with ".tmp" maybe temp dir or litter dir
                    if((!checkpoint_dir.getName().endsWith(".tmp")) && checkpoint_dir.getName().startsWith(prefix)) {
                        if (checkpoint_dir.getName().compareTo(latest_checkpoint_name) > 0) {
                            latest_checkpoint_name = checkpoint_dir.getName();
                        }
                    }
                }
            }
        }
        return latest_checkpoint_name;
    }

    /**
     * Purge Old checkpoint, only retain latest [count] checkpoints.
     *
     * @param count Count of checkpoint to retain
     *
     * @throws RuntimeException
     */
    public void purgeOldCheckpoint(int count) {
        if (destroy || disable_checkpoint_purge) {
            return;
        }
        try {
            // Sort directory names by alphabetical order
            Comparator<File> comparatorFileName = new Comparator<File>(){
                public int compare(File p1,File p2){
                    return p2.getName().compareTo(p1.getName());
                }
            };

            File[] directories = new File(this.checkpointPath.toString()).listFiles(File::isDirectory);
            Arrays.sort(directories, comparatorFileName);

            // Delete old checkpoint directories
            int persistCount = 0;
            if (directories.length > count) {
                for (int i = 0; i < directories.length; i++) {
                    // dir name end with ".tmp" is delayed to delete
                    if(!directories[i].getName().endsWith(".tmp")) {
                        persistCount++;
                    }

                    if(persistCount > count){
                        FileUtils.deleteIfExists(directories[i].toPath());
                    }
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Restore db from latest checkpoint
     *
     * @throws RuntimeException
     */
    public void restoreFromLatestCheckpoint() {
        log.info("RocksStorage::restoreFromLatestCheckpoint");
        if (destroy) {
            throw new RuntimeException();
        }
        try {
            String checkpoint_name = String.format("%s%s", REMOTE_CHECKPOINT_PREFIX, "checkpoint");
            if (checkpoint_name.length() == 0){
                throw new RuntimeException("GetLatestCheckpointName return null string");
            }
            log.info("RocksStorage::restoreFromLatestCheckpoint  checkpoint_name=" + checkpoint_name);

            //1.generate temp new db dir for new RocksDB
            Path temp_new_db_path = this.path.resolve("load_from_"+ checkpoint_name);
            Path temp_old_db_path = this.path.resolve("will_delete_soon_"+ checkpoint_name);

            //2.rename remote checkpoint to temp_new_db_path
            Files.move(this.checkpointPath.resolve(checkpoint_name), temp_new_db_path);

            //3.rename old db to will_delete_soon_[checkpoint_name]
            checkpoint.close();
            checkpoint = null;
            closeDB();
            Files.move(this.dbPath, temp_old_db_path);

            //4.rename temp new db dir to new db dir
            Files.move(temp_new_db_path, this.dbPath);

            //5.createDB()
            db = createDB();
            checkpoint = Checkpoint.create(db);

            //6.delete old db thoroughly
            FileUtils.deleteIfExists(temp_old_db_path);
            log.info("RocksStorage::restoreFromLatestCheckpoint  finished =" + checkpoint_name);

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void backup() {
        if (use_checkpoint) {
            createNewCheckpoint();
            purgeOldCheckpoint(3);
            return;
        }

        if (destroy) {
            return;
        }
        try {
            if (backup != null) {
                backup.createNewBackup(db);
                backup.purgeOldBackups(3);
                backup.garbageCollect();
            }
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String receiveBackup() {
        if (use_checkpoint){
            String checkpoint_name = String.format("%s%s", REMOTE_CHECKPOINT_PREFIX, "checkpoint");
            log.info(String.format("receiveBackup path=[%s]\n", this.checkpointPath.resolve(checkpoint_name).toString()));

            FileUtils.deleteIfExists(this.path.resolve(checkpoint_name));
            FileUtils.createDirectories(this.path.resolve(checkpoint_name));

            return this.dbPath.resolve(checkpoint_name).toString();
        } else {
            return this.backupPath.toString();
        }
    }

    @Override
    public void applyBackup() {
        if (use_checkpoint){
            restoreFromLatestCheckpoint();
            return;
        }

        if (destroy) {
            throw new RuntimeException();
        }
        try {
            backup.close();
            backup = BackupEngine.open(db.getEnv(), new BackupEngineOptions(backupPath.toString()));
            closeDB();
            backup.restoreDbFromLatestBackup(
                dbPath.toString(), dbPath.resolve("wal").toString(), new RestoreOptions(false)
            );
            db = createDB();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public long approximateCount() {
        try {
            if (destroy) {
                throw new RuntimeException();
            }
            return db.getLongProperty(dcfHandler, "rocksdb.estimate-num-keys");
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public long approximateSize() {
        if (destroy) {
            throw new RuntimeException();
        }
        try (
            Snapshot snapshot = db.getSnapshot();
            ReadOptions readOptions = new ReadOptions().setSnapshot(snapshot)
        ) {
            try (final RocksIterator it = this.db.newIterator(readOptions)) {
                Slice start = null;
                Slice limit = null;
                it.seekToFirst();
                if (it.isValid()) {
                    start = new Slice(it.key());
                }
                it.seekToLast();
                if (it.isValid()) {
                    limit = new Slice(it.key());
                }
                if (start != null && limit != null) {
                    return Arrays.stream(
                        db.getApproximateSizes(singletonList(new Range(start, limit)), INCLUDE_FILES, INCLUDE_MEMTABLES)
                    ).sum();
                }
            } finally {
                readOptions.setSnapshot(null);
                db.releaseSnapshot(snapshot);
            }
        }
        return 0;
    }

    @Override
    public void clearClock(long clock) {
        if (destroy) {
            throw new RuntimeException();
        }
        try {
            instruction.delete(icfHandler, encodeLong(clock));
            if (clock % 1000000 == 0) {
                instruction.deleteRange(icfHandler, encodeLong(0), encodeLong(clock));
            }
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public long clocked() {
        if (destroy) {
            throw new RuntimeException();
        }
        try {
            return Optional.mapOrGet(db.get(mcfHandler, CLOCK_K), PrimitiveCodec::readLong, () -> 0L);
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public long clock() {
        if (destroy) {
            throw new RuntimeException();
        }
        try {
            return Optional.mapOrGet(instruction.get(icfHandler, CLOCK_K), PrimitiveCodec::readLong, () -> 0L);
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void tick(long clock) {
        if (destroy) {
            throw new RuntimeException();
        }
        try {
            this.instruction.put(icfHandler, CLOCK_K, PrimitiveCodec.encodeLong(clock));
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void saveInstruction(long clock, byte[] instruction) {
        if (destroy) {
            throw new RuntimeException();
        }
        try {
            this.instruction.put(icfHandler, PrimitiveCodec.encodeLong(clock), instruction);
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public byte[] reappearInstruction(long clock) {
        if (destroy) {
            throw new RuntimeException();
        }
        try {
            return instruction.get(icfHandler, PrimitiveCodec.encodeLong(clock));
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String filePath() {
        return null;
    }

    @Override
    public Reader reader() {
        if (destroy) {
            throw new RuntimeException();
        }
        return new Reader(db, dcfHandler);
    }

    @Override
    public Writer writer(Instruction instruction) {
        if (destroy) {
            throw new RuntimeException();
        }
        return new Writer(db, instruction, dcfHandler);
    }

    @Override
    public void flush(io.dingodb.mpu.storage.Writer writer) {
        if (destroy) {
            throw new RuntimeException();
        }
        try {
            Instruction instruction = writer.instruction();
            WriteBatch batch = ((Writer) writer).writeBatch();
            byte[] clockValue = PrimitiveCodec.encodeLong(instruction.clock);
            if (RocksUtils.ttlValid(this.ttl)) {
                clockValue = RocksUtils.getValueWithNowTs(clockValue);
            }
            batch.put(mcfHandler, CLOCK_K, clockValue);
            this.db.write(writeOptions, batch);
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            try {
                writer.close();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    private void compact() {
        long now = System.currentTimeMillis();
        try {
            if (this.db != null) {
                this.db.compactRange();
            } else {
                log.info("RocksStorage compact db is null.");
            }
        } catch (final Exception e) {
            log.error("RocksStorage compact exception, label: {}.", this.coreMeta.label, e);
            throw new RuntimeException(e);
        }
        log.info("RocksStorage compact, label: {}, cost {}s.", this.coreMeta.label,
            (System.currentTimeMillis() - now) / 1000 );
    }

    private static ColumnFamilyDescriptor dcfDesc() {
        final ColumnFamilyOptions cfOption = new ColumnFamilyOptions();
        /**
         * configuration for performance.
         * write_buffer_size: will control the sst file size
         */
        final long blockSize = 128L * 1024;
        final long targetFileSize = 256L * 1024 * 1024;
        cfOption.setMaxWriteBufferNumber(5);
        cfOption.setWriteBufferSize(targetFileSize);
        cfOption.setMaxBytesForLevelBase(1L * 1024 * 1024 * 1024);
        cfOption.setTargetFileSizeBase(64L * 1024 * 1024);
        cfOption.setMinWriteBufferNumberToMerge(1);

        List<CompressionType> compressionTypes = Arrays.asList(
            CompressionType.NO_COMPRESSION,
            CompressionType.NO_COMPRESSION,
            CompressionType.LZ4_COMPRESSION,
            CompressionType.LZ4_COMPRESSION,
            CompressionType.LZ4_COMPRESSION,
            CompressionType.ZSTD_COMPRESSION,
            CompressionType.ZSTD_COMPRESSION);
        cfOption.setCompressionPerLevel(compressionTypes);

        BlockBasedTableConfig tableConfig = new BlockBasedTableConfig();
        tableConfig.setBlockSize(blockSize);

        tableConfig.setFilterPolicy(new BloomFilter(MAX_BLOOM_HASH_NUM, false));
        tableConfig.setWholeKeyFiltering(true);
        // tableConfig.setCacheIndexAndFilterBlocks(true);
        cfOption.useCappedPrefixExtractor(MAX_PREFIX_LENGTH);

        Cache blockCache = new LRUCache(1 * 1024 * 1024 * 1024);
        tableConfig.setBlockCache(blockCache);
        Cache compressedBlockCache = new LRUCache(1 * 1024 * 1024 * 1024);
        tableConfig.setBlockCacheCompressed(compressedBlockCache);
        cfOption.setTableFormatConfig(tableConfig);
        return new ColumnFamilyDescriptor(CF_DEFAULT, cfOption);
    }

    private static ColumnFamilyDescriptor icfDesc() {
        return new ColumnFamilyDescriptor(CF_DEFAULT, new ColumnFamilyOptions());
    }

    private static ColumnFamilyDescriptor icfDesc(ColumnFamilyOptions cfOptions) {
        return new ColumnFamilyDescriptor(CF_DEFAULT, cfOptions);
    }

    private static ColumnFamilyDescriptor mcfDesc() {
        return new ColumnFamilyDescriptor(CF_META, new ColumnFamilyOptions());
    }

    public class Listener extends AbstractEventListener {

        @Override
        public void onFlushCompleted(RocksDB db, FlushJobInfo flushJobInfo) {
            log.info("{} on flush completed, info: {}", coreMeta.label, flushJobInfo);
            if (
                db.getName().equals(RocksStorage.this.db.getName())
                && flushJobInfo.getColumnFamilyId() == dcfHandler.getID()
                && !RocksStorage.this.destroy
            ) {
                log.info("Flush on db default, will flush instruction and meta, and backup default db.");
                runner.forceFollow(() -> LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(1)));
                runner.forceFollow(RocksStorage.this::flushMeta);
                runner.forceFollow(RocksStorage.this::backup);
            }
        }

        @Override
        public void onFlushBegin(RocksDB db, FlushJobInfo flushJobInfo) {
            log.info("{} on flush begin, info: {}", coreMeta.label, flushJobInfo);
        }

        @Override
        public void onTableFileDeleted(TableFileDeletionInfo tableFileDeletionInfo) {
            log.info("{} on table file deleted, info: {}", coreMeta.label, tableFileDeletionInfo);
            //runner.forceFollow(RocksStorage.this::backup);
        }

        @Override
        public void onCompactionBegin(RocksDB db, CompactionJobInfo compactionJobInfo) {
            log.info("{} on compaction begin, info: {}", coreMeta.label, compactionJobInfo);
        }

        @Override
        public void onCompactionCompleted(RocksDB db, CompactionJobInfo compactionJobInfo) {
            log.info("{} on compaction completed, info: {}", coreMeta.label, compactionJobInfo);
            if (
                db.getName().equals(RocksStorage.this.db.getName())
                && Arrays.equals(compactionJobInfo.columnFamilyName(), CF_DEFAULT)
                && !RocksStorage.this.destroy
            ) {
                runner.forceFollow(() -> LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(1)));
                runner.forceFollow(RocksStorage.this::backup);
            }
        }

        @Override
        public void onTableFileCreated(TableFileCreationInfo tableFileCreationInfo) {
            log.info("{} on table file created, info: {}", coreMeta.label, tableFileCreationInfo);
        }

        @Override
        public void onTableFileCreationStarted(TableFileCreationBriefInfo tableFileCreationBriefInfo) {
            log.info("{} on table file creation started, info: {}", coreMeta.label, tableFileCreationBriefInfo);
        }

        @Override
        public void onMemTableSealed(MemTableInfo memTableInfo) {
            log.info("{} on mem table sealed, info: {}", coreMeta.label, memTableInfo);
        }

        @Override
        public void onBackgroundError(BackgroundErrorReason reason, Status status) {
            log.error(
                "{} on background error, reason: {}, code: {}, state: {}",
                coreMeta.label, reason, status.getCodeString(), status.getState()
            );
        }

        @Override
        public void onStallConditionsChanged(WriteStallInfo writeStallInfo) {
            log.info("{} on stall conditions changed, info: {}", coreMeta.label, writeStallInfo);
        }

        @Override
        public void onFileReadFinish(FileOperationInfo fileOperationInfo) {
            log.info("{} on file read finish, info: {}", coreMeta.label, fileOperationInfo);
        }

        @Override
        public void onFileWriteFinish(FileOperationInfo fileOperationInfo) {
            log.info("{} on file write finish, info: {}", coreMeta.label, fileOperationInfo);
        }

        @Override
        public void onFileFlushFinish(FileOperationInfo fileOperationInfo) {
            log.info("{} on file flush finish, info: {}", coreMeta.label, fileOperationInfo);
        }

        @Override
        public void onFileSyncFinish(FileOperationInfo fileOperationInfo) {
            log.info("{} on file sync finish, info: {}", coreMeta.label, fileOperationInfo);
        }

        @Override
        public void onFileRangeSyncFinish(FileOperationInfo fileOperationInfo) {
            log.info("{} on file range sync finish, info: {}", coreMeta.label, fileOperationInfo);
        }

        @Override
        public void onFileTruncateFinish(FileOperationInfo fileOperationInfo) {
            log.info("{} on file truncate finish, info: {}", coreMeta.label, fileOperationInfo);
            //runner.forceFollow(RocksStorage.this::backup);
        }

        @Override
        public void onFileCloseFinish(FileOperationInfo fileOperationInfo) {
            log.info("{} on file close finish, info: {}", coreMeta.label, fileOperationInfo);
        }

        @Override
        public boolean onErrorRecoveryBegin(BackgroundErrorReason reason, Status status) {
            log.info(
                "{} on error recovery begin, reason: {}, code: {}, state: {}",
                coreMeta.label, reason, status.getCodeString(), status.getState()
            );
            return super.onErrorRecoveryBegin(reason, status);
        }

        @Override
        public void onErrorRecoveryCompleted(Status status) {
            log.info(
                "{} on error recovery completed, code: {}, state: {}",
                coreMeta.label, status.getCodeString(), status.getState()
            );
        }

    }
}
