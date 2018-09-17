using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;
using EventStore.ClientAPI;
using EventStore.ClientAPI.Projections;
using Newtonsoft.Json;
using ReactiveDomain.Logging;
using ReactiveDomain.Util;
using ES_ILogger = EventStore.ClientAPI.ILogger;
using ILogger = ReactiveDomain.Logging.ILogger;


namespace ReactiveDomain.EventStore {
    public class EventStoreLoader {
        public enum StartConflictOption {
            Kill,
            Connect,
            Error
        }

        private readonly ILogger _log = LogManager.GetLogger("Common");

        private static readonly JsonSerializerSettings SerializerSettings = new JsonSerializerSettings { TypeNameHandling = TypeNameHandling.None };
        private Process _process;
        public ProjectionsManager ProjectionsManager;

        public IStreamStoreConnection Connection { get; private set; }

        public void SetupEventStore(DirectoryInfo installPath, string additionalArgs = null) {
            var args = $" --config=\"./config.yaml\" {additionalArgs ?? ""}";

            SetupEventStore(installPath,
                args,
                new UserCredentials("admin", "changeit"),
                IPAddress.Parse("127.0.0.1"),
                tcpPort: 1113,
                windowStyle: ProcessWindowStyle.Hidden,
                opt: StartConflictOption.Connect);
        }
        public void SetupEventStore(
                                DirectoryInfo installPath,
                                string args,
                                UserCredentials credentials,
                                IPAddress server,
                                int tcpPort,
                                ProcessWindowStyle windowStyle,
                                StartConflictOption opt) {
            Ensure.NotNullOrEmpty(args, "args");
            Ensure.NotNull(credentials, "credentials");

            var fullPath = Path.Combine(installPath.FullName, "EventStore.ClusterNode.exe");

            var runningEventStores = Process.GetProcessesByName("EventStore.ClusterNode");
            if (runningEventStores.Count() != 0) {
                switch (opt) {
                    case StartConflictOption.Connect:
                        _process = runningEventStores[0];
                        break;
                    case StartConflictOption.Kill:
                        foreach (var es in runningEventStores) {
                            es.Kill();
                        }
                        break;
                    case StartConflictOption.Error:
                        throw new Exception("Conflicting EventStore running.");
                }
            }

            if (_process == null) {
                _process = new Process {
                    StartInfo = {
                                    WindowStyle = windowStyle,
                                    UseShellExecute = true,
                                    CreateNoWindow = false,
                                    WorkingDirectory = installPath.FullName,
                                    FileName = fullPath,
                                    Arguments = args,
                                    Verb ="runas"
                                }
                };
                _process.Start();
            }
            Connect(
                credentials,
                server,
                tcpPort);
        }

        /// <summary>
        /// Establish EventStore Cluster Connection via Discovery
        /// </summary>
        /// <remarks>
        /// Define a common DNS name relating it to all cluster node ID address(es).
        /// EventStore will process the DNS into gossip seeds for use in the connection.
        /// </remarks>
        /// <param name="credentials">UserCredentials</param>
        /// <param name="dnsName">DNS name representing cluster IP address(es)</param>
        /// <param name="tcpPort">TCP port used for all cluster nodes</param>
        public void Connect(
            UserCredentials credentials,
            string dnsName,
            int tcpPort) {

            if (dnsName is null || dnsName.Length.Equals(0)) {
                _log.Error("The DNS name must be supplied.");
                return;
            }

            var settings = ConnectionSettings.Create()
                .SetDefaultUserCredentials(new global::EventStore.ClientAPI.SystemData.UserCredentials(credentials.Username, credentials.Password))
                .KeepReconnecting()
                .KeepRetrying()
                .UseConsoleLogger()
                //.EnableVerboseLogging()
                .Build();

            var esConn = EventStoreConnection.Create(settings,
                ClusterSettings.Create()
                .DiscoverClusterViaDns()
	            .SetClusterDns(dnsName)
	            .SetClusterGossipPort(tcpPort), $"{dnsName}-Cluster Connection");

            Connection = new EventStoreConnectionWrapper(esConn);

            if (Connection == null) {
                _log.Error("EventStore DNS Cluster Connection is null - Diagnostic Monitoring will be unavailable.");
                TeardownEventStore(false);
                return;
            }
            StartEventStore();
        }

        /// <summary>
        /// Establish EventStore Cluster Connection via Gossip Seed IP address(es) and the same TCP port
        /// </summary>
        /// <remarks>
        /// Connect to an EventStore cluster using gossip seed IP addresses.
        /// This supports both a single EventStore cluster node and a multi-node EventStore cluster.
        /// A cluster of 1 is equivalent to a single instance.
        /// </remarks>
        /// <param name="credentials">UserCredentials</param>
        /// <param name="gossipSeeds">The TCP/IP addresses of the EventStore servers</param>
        /// <param name="tcpPort">TCP ports replicated for all gossip seeds.</param>
        public void Connect(
            UserCredentials credentials,
            IPAddress[] gossipSeeds,
            int tcpPort) {

            if (gossipSeeds.Length == 0) {
                _log.Error("One or more EventStore Gossip Seed IPs must be supplied.");
                return;
            }

	        var ports = new List<int>();
	        for (var i = 0; i < gossipSeeds.Length; i++)
	        {
		        ports.Add(tcpPort);
	        }
			Connect(credentials, gossipSeeds, ports.ToArray());
        }

		/// <summary>
		/// Establish EventStore Cluster Connection via Gossip Seed IP address(es)
		/// </summary>
		/// <remarks>
		/// Connect to an EventStore cluster using gossip seed IP addresses.
		/// This supports both a single EventStore cluster node and a multi-node EventStore cluster.
		/// A cluster of 1 is equivalent to a single instance.
		/// </remarks>
		/// <param name="credentials">UserCredentials</param>
		/// <param name="gossipSeeds">The TCP/IP addresses of the EventStore servers</param>
		/// <param name="tcpPorts">TCP ports for the gossip seeds. Keep in order with the gossip seeds. If all gossip seeds use the same port, you can pass only one.</param>
		public void Connect(
			UserCredentials credentials,
			IPAddress[] gossipSeeds,
			int[] tcpPorts)
		{

			if (gossipSeeds.Length == 0)
			{
				_log.Error("One or more EventStore Gossip Seed IPs must be supplied.");
				return;
			}
			if (tcpPorts.Length == 0)
			{
				_log.Error("One or more TCP ports must be supplied.");
				return;
			}
			if (gossipSeeds.Length != tcpPorts.Length && tcpPorts.Length > 1)
			{
				_log.Error("One TCP port, or one port per seed is required. ");
			}

			var settings = ConnectionSettings.Create()
				.SetDefaultUserCredentials(new global::EventStore.ClientAPI.SystemData.UserCredentials(credentials.Username, credentials.Password))
				.KeepReconnecting()
				.KeepRetrying()
				.UseConsoleLogger()
				//.EnableVerboseLogging()
				.Build();

			var seeds = new List<IPEndPoint>();
			for (var i = 0; i < gossipSeeds.Length; i++)
			{
				seeds.Add(new IPEndPoint(gossipSeeds[i], tcpPorts.Length.Equals(1) ? tcpPorts[0] : tcpPorts[i]));
			}

			Connection = new EventStoreConnectionWrapper(
				EventStoreConnection.Create(settings,
					ClusterSettings.Create()
					.DiscoverClusterViaGossipSeeds()
					.SetGossipSeedEndPoints(seeds.ToArray()), $"{gossipSeeds.Length}-Cluster Connection"));

			if (Connection == null)
			{
				_log.Error($"EventStore Custer of {gossipSeeds.Length} Connection is null - Diagnostic Monitoring will be unavailable.");
				TeardownEventStore(false);
				return;
			}
			StartEventStore();
		}

		/// <summary>
		/// Connect to EventStore with an IP address and a port
		/// </summary>
		/// <param name="credentials">UserCredentials</param>
		/// <param name="server">IP address of the EventStore server</param>
		/// <param name="tcpPort">TCP port on the server</param>
		public void Connect(
                        UserCredentials credentials,
                        IPAddress server,
                        int tcpPort) {
            var tcpEndpoint = new IPEndPoint(server, tcpPort);

            var settings = ConnectionSettings.Create()
                .SetDefaultUserCredentials(new global::EventStore.ClientAPI.SystemData.UserCredentials(credentials.Username, credentials.Password))
                .KeepReconnecting()
                .KeepRetrying()
                .UseConsoleLogger()
                //.EnableVerboseLogging()
                .Build();

            Connection =
                new EventStoreConnectionWrapper(EventStoreConnection.Create(settings, tcpEndpoint, "Default Connection"));

            if (Connection == null)
            {
                _log.Error("EventStore Connection is null - Diagnostic Monitoring will be unavailable.");
                TeardownEventStore(false);
                return;
            }
            StartEventStore();
        }

        /// <summary>
        /// Connect to EventStore and test the connection
        /// </summary>
        /// <remarks>
        /// 
        /// </remarks>
        private void StartEventStore()
        {
            Connection.Connect();
            int retry = 8;
            int count = 0;
            do
            {
                try
                {
                    Connection.ReadStreamForward("by_event_type", 0, 1);
                    return;
                }
                catch
                {
                    //ignore
                }

                Thread.Sleep(100);
                count++;
            } while (count < retry);

            throw new Exception("Unable to start Eventstore");
        }

        public static EventData ToEventData(
            Guid eventId,
            object message,
            Dictionary<string, object> metaData) {
            dynamic typedMessage = message;
            var eventBytes = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(typedMessage, SerializerSettings));
            var metaDataString = JsonConvert.SerializeObject(metaData, SerializerSettings);
            //Log.Debug("Metadata= " + metaDataString);
            var metaDataBytes = Encoding.UTF8.GetBytes(metaDataString);
            var typeName = typedMessage.GetType().Name;
            return new EventData(eventId, typeName, true, eventBytes, metaDataBytes);
        }
        //
        //N.B. if we need this the use of deserialization via type id is unsafe and the full type name should be used instead
        //
        //public static void FromEventData(
        //    RecordedEvent recordedEvent,
        //    out dynamic message,
        //    out Dictionary<string, object> metaData)
        //{
        //    string metaDataString = Encoding.UTF8.GetString(recordedEvent.Metadata);
        //    metaData = JsonConvert.DeserializeObject<Dictionary<string, object>>(metaDataString, SerializerSettings);
        //    string eventString = Encoding.UTF8.GetString(recordedEvent.Data);
        //    if (metaData.ContainsKey("MsgTypeId"))
        //    {
        //        var msgTypeId = (long)metaData["MsgTypeId"];
        //        if (MessageHierarchy.MsgTypeByTypeId.ContainsKey((int)msgTypeId))
        //        {
        //            var msgType = MessageHierarchy.MsgTypeByTypeId[(int)msgTypeId];
        //            message = JsonConvert.DeserializeObject(eventString, msgType, SerializerSettings);
        //            return;
        //        }
        //    }
        //    message = JsonConvert.DeserializeObject(eventString, SerializerSettings);
        //    //Log.Warn("metaData did not contain MsgTypeId, and thus FromEventData() could not deserialize event as the correct object-type");
        //}

        public void TeardownEventStore(bool leaveRunning = true) {
            Connection?.Close();
            if (leaveRunning || _process == null || _process.HasExited) return;
            _process.Kill();
            _process.WaitForExit();
        }
    }
    public class NullLogger : ES_ILogger {
        public void Debug(string format, params object[] args) { }

        public void Debug(Exception ex, string format, params object[] args) { }

        public void Error(string format, params object[] args) { }

        public void Error(Exception ex, string format, params object[] args) { }

        public void Info(string format, params object[] args) { }

        public void Info(Exception ex, string format, params object[] args) { }
    }
}