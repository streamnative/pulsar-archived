/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.admin.cli;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.pulsar.cli.converters.TimeUnitToMillisConverter;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.common.policies.data.TransactionCoordinatorInfo;

@Parameters(commandDescription = "Operations on transactions")
public class CmdTransactions extends CmdBase {

    @Parameters(commandDescription = "Get transaction coordinator stats")
    private class GetCoordinatorStats extends CliCommand {
        @Parameter(names = {"-c", "--coordinator-id"}, description = "The coordinator id", required = false)
        private Integer coordinatorId;

        @Override
        void run() throws Exception {
            if (coordinatorId != null) {
                print(getAdmin().transactions().getCoordinatorStatsById(coordinatorId));
            } else {
                print(getAdmin().transactions().getCoordinatorStats());
            }
        }
    }

    @Parameters(commandDescription = "Get transaction buffer stats")
    private class GetTransactionBufferStats extends CliCommand {
        @Parameter(names = {"-t", "--topic"}, description = "The topic", required = true)
        private String topic;

        @Parameter(names = {"-l", "--low-water-mark"},
                description = "Whether to get information about lowWaterMarks stored in transaction buffer.")
        private boolean lowWaterMark;

        @Parameter(names = {"-s", "--segment-stats"},
                description = "Whether to get segment statistics.")
        private boolean segmentStats = false;

        @Override
        void run() throws Exception {
            // Assuming getTransactionBufferStats method signature has been updated to accept the new parameter
            print(getAdmin().transactions().getTransactionBufferStats(topic, lowWaterMark, segmentStats));
        }
    }

    @Parameters(commandDescription = "Get transaction pending ack stats")
    private class GetPendingAckStats extends CliCommand {
        @Parameter(names = {"-t", "--topic"}, description = "The topic name", required = true)
        private String topic;

        @Parameter(names = {"-s", "--sub-name"}, description = "The subscription name", required = true)
        private String subName;

        @Parameter(names = {"-l", "--low-water-mark"},
                description = "Whether to get information about lowWaterMarks stored in transaction pending ack.")
        private boolean lowWaterMarks;

        @Override
        void run() throws Exception {
            print(getAdmin().transactions().getPendingAckStats(topic, subName, lowWaterMarks));
        }
    }

    @Parameters(commandDescription = "Get transaction in pending ack stats")
    private class GetTransactionInPendingAckStats extends CliCommand {
        @Parameter(names = {"-m", "--most-sig-bits"}, description = "The most sig bits", required = true)
        private int mostSigBits;

        @Parameter(names = {"-l", "--least-sig-bits"}, description = "The least sig bits", required = true)
        private long leastSigBits;

        @Parameter(names = {"-t", "--topic"}, description = "The topic name", required = true)
        private String topic;

        @Parameter(names = {"-s", "--sub-name"}, description = "The subscription name", required = true)
        private String subName;

        @Override
        void run() throws Exception {
            print(getAdmin().transactions().getTransactionInPendingAckStats(new TxnID(mostSigBits, leastSigBits),
                    topic, subName));
        }
    }


    @Parameters(commandDescription = "Get transaction in buffer stats")
    private class GetTransactionInBufferStats extends CliCommand {
        @Parameter(names = {"-m", "--most-sig-bits"}, description = "The most sig bits", required = true)
        private int mostSigBits;

        @Parameter(names = {"-l", "--least-sig-bits"}, description = "The least sig bits", required = true)
        private long leastSigBits;

        @Parameter(names = {"-t", "--topic"}, description = "The topic name", required = true)
        private String topic;

        @Override
        void run() throws Exception {
            print(getAdmin().transactions().getTransactionInBufferStats(new TxnID(mostSigBits, leastSigBits), topic));
        }
    }

    @Parameters(commandDescription = "Get transaction metadata")
    private class GetTransactionMetadata extends CliCommand {
        @Parameter(names = {"-m", "--most-sig-bits"}, description = "The most sig bits", required = true)
        private int mostSigBits;

        @Parameter(names = {"-l", "--least-sig-bits"}, description = "The least sig bits", required = true)
        private long leastSigBits;

        @Override
        void run() throws Exception {
            print(getAdmin().transactions().getTransactionMetadata(new TxnID(mostSigBits, leastSigBits)));
        }
    }

    @Parameters(commandDescription = "Get slow transactions.")
    private class GetSlowTransactions extends CliCommand {
        @Parameter(names = {"-c", "--coordinator-id"}, description = "The coordinator id", required = false)
        private Integer coordinatorId;

        @Parameter(names = { "-t", "--time" }, description = "The transaction timeout time. "
                + "(eg: 1s, 10s, 1m, 5h, 3d)", required = true,
                converter = TimeUnitToMillisConverter.class)
        private Long timeoutInMillis = 1L;

        @Override
        void run() throws Exception {
            if (coordinatorId != null) {
                print(getAdmin().transactions().getSlowTransactionsByCoordinatorId(coordinatorId,
                        timeoutInMillis, TimeUnit.MILLISECONDS));
            } else {
                print(getAdmin().transactions().getSlowTransactions(timeoutInMillis, TimeUnit.MILLISECONDS));
            }
        }
    }

    @Parameters(commandDescription = "Get transaction coordinator internal stats")
    private class GetCoordinatorInternalStats extends CliCommand {
        @Parameter(names = {"-c", "--coordinator-id"}, description = "The coordinator id", required = true)
        private int coordinatorId;

        @Parameter(names = { "-m", "--metadata" }, description = "Flag to include ledger metadata")
        private boolean metadata = false;
        @Override
        void run() throws Exception {
            print(getAdmin().transactions().getCoordinatorInternalStats(coordinatorId, metadata));
        }
    }

    @Parameters(commandDescription = "Get pending ack internal stats")
    private class GetPendingAckInternalStats extends CliCommand {
        @Parameter(names = {"-t", "--topic"}, description = "Topic name", required = true)
        private String topic;

        @Parameter(names = {"-s", "--subscription-name"}, description = "Subscription name", required = true)
        private String subName;

        @Parameter(names = { "-m", "--metadata" }, description = "Flag to include ledger metadata")
        private boolean metadata = false;
        @Override
        void run() throws Exception {
            print(getAdmin().transactions().getPendingAckInternalStats(topic, subName, metadata));
        }
    }

    @Parameters(commandDescription = "Get transaction buffer internal stats")
    private class GetTransactionBufferInternalStats extends CliCommand {
        @Parameter(names = {"-t", "--topic"}, description = "Topic name", required = true)
        private String topic;

        @Parameter(names = { "-m", "--metadata" }, description = "Flag to include ledger metadata")
        private boolean metadata = false;

        @Override
        void run() throws Exception {
            print(getAdmin().transactions().getTransactionBufferInternalStats(topic, metadata));
        }
    }

    @Parameters(commandDescription = "Update the scale of transaction coordinators")
    private class ScaleTransactionCoordinators extends CliCommand {
        @Parameter(names = { "-r", "--replicas" }, description = "The scale of the transaction coordinators")
        private int replicas;
        @Override
        void run() throws Exception {
            getAdmin().transactions().scaleTransactionCoordinators(replicas);
        }
    }

    @Parameters(commandDescription = "Get the position stats in transaction pending ack")
    private class GetPositionStatsInPendingAck extends CliCommand {
        @Parameter(names = {"-t", "--topic"}, description = "The topic name", required = true)
        private String topic;

        @Parameter(names = {"-s", "--subscription-name"}, description = "Subscription name", required = true)
        private String subName;

        @Parameter(names = {"-l", "--ledger-id"}, description = "Ledger ID of the position", required = true)
        private Long ledgerId;

        @Parameter(names = {"-e", "--entry-id"}, description = "Entry ID of the position", required = true)
        private Long entryId;

        @Parameter(names = {"-b", "--batch-index"}, description = "Batch index of the position")
        private Integer batchIndex;

        @Override
        void run() throws Exception {
            getAdmin().transactions().getPositionStatsInPendingAck(topic, subName, ledgerId, entryId, batchIndex);
        }
    }

    @Parameters(commandDescription = "List transaction coordinators")
    private class ListTransactionCoordinators extends CliCommand {
        @Override
        void run() throws Exception {
            print(getAdmin()
                    .transactions()
                    .listTransactionCoordinators()
                    .stream()
                    .collect(Collectors.toMap(
                            TransactionCoordinatorInfo::getId,
                            TransactionCoordinatorInfo::getBrokerServiceUrl
                    ))
            );
        }
    }

    @Parameters(commandDescription = "Abort transaction")
    private class AbortTransaction extends CliCommand {
        @Parameter(names = {"-m", "--most-sig-bits"}, description = "The most sig bits", required = true)
        private long mostSigBits;

        @Parameter(names = {"-l", "--least-sig-bits"}, description = "The least sig bits", required = true)
        private long leastSigBits;

        @Override
        void run() throws Exception {
            getAdmin().transactions().abortTransaction(new TxnID(mostSigBits, leastSigBits));
        }
    }

    public CmdTransactions(Supplier<PulsarAdmin> admin) {
        super("transactions", admin);
        jcommander.addCommand("coordinator-internal-stats", new GetCoordinatorInternalStats());
        jcommander.addCommand("pending-ack-internal-stats", new GetPendingAckInternalStats());
        jcommander.addCommand("buffer-snapshot-internal-stats", new GetTransactionBufferInternalStats());
        jcommander.addCommand("coordinator-stats", new GetCoordinatorStats());
        jcommander.addCommand("transaction-buffer-stats", new GetTransactionBufferStats());
        jcommander.addCommand("pending-ack-stats", new GetPendingAckStats());
        jcommander.addCommand("transaction-in-buffer-stats", new GetTransactionInBufferStats());
        jcommander.addCommand("transaction-in-pending-ack-stats", new GetTransactionInPendingAckStats());
        jcommander.addCommand("transaction-metadata", new GetTransactionMetadata());
        jcommander.addCommand("slow-transactions", new GetSlowTransactions());
        jcommander.addCommand("scale-transactionCoordinators", new ScaleTransactionCoordinators());
        jcommander.addCommand("position-stats-in-pending-ack", new GetPositionStatsInPendingAck());
        jcommander.addCommand("coordinators-list", new ListTransactionCoordinators());
        jcommander.addCommand("abort-transaction", new AbortTransaction());

    }
}
