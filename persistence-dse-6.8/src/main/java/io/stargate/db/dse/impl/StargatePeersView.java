package io.stargate.db.dse.impl;

import org.apache.cassandra.db.virtual.DataSet;

import com.datastax.bdp.db.nodes.virtual.NodesSystemViews;
import com.datastax.bdp.db.nodes.virtual.PeersSystemView;

public class StargatePeersView extends StargateNodeView
{
    StargatePeersView()
    {
        super(NodesSystemViews.virtualFromLegacy(NodesSystemViews.Peers, PeersSystemView.NAME));
    }

    @Override
    public DataSet data()
    {
        DataSet result = newDataSet();

        for (StargatePeerInfo info: StargateSystemKeyspace.instance.getPeers().values())
        {
            info = info.copy();

            result.addRow(info.getPeer(),
                    completeRow(result.newRowBuilder()
                            //+ "preferred_ip inet,"
                            .addColumn("preferred_ip", info::getRpcAddress), info));
        }

        return result;
    }
}