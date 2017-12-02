﻿using System;
using System.Windows.Forms;
using System.Collections.Generic;
using Messages;
using StackExchange.Redis;
using Newtonsoft.Json;
using System.Diagnostics;

namespace Client
{
    public partial class Form1 : Form
    {
       ConnectionMultiplexer _redis;
        IDatabase _db;

        public Form1()
        {
            InitializeComponent();
            Load += Form1_Load;
        }

        private void Form1_Load(object sender, EventArgs e)
        {
            string redisConnectionString = Environment.GetEnvironmentVariable("REDIS_CONNECTIONSTRING", EnvironmentVariableTarget.Machine);
            _redis = ConnectionMultiplexer.Connect(redisConnectionString);
            _db = _redis.GetDatabase();
            ISubscriber sub = _redis.GetSubscriber();
            
            sub.Subscribe("GroupedDataMessages", (channel, val) =>
            {
                PrintMessage(val);

            }, CommandFlags.HighPriority);
        }

        public void PrintMessage(string msg)
        {
            Stopwatch sw = new Stopwatch();
            sw.Start();
            this.Invoke((MethodInvoker)(() =>
            {
                _dataDisplay.Items.Add(msg);

            }));
            sw.Stop();
            Debug.WriteLine($"{sw.ElapsedMilliseconds}");

        }
    }
}

