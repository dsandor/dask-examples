from dask_cloudprovider.azure import AzureVMCluster
import time

cluster = AzureVMCluster(resource_group="DaskGroup",
                         vnet="DaskVnet",
                         location="eastus",
                         security_group="DashSecGroup",
                         vm_size="Standard_D4s_v3",
                         n_workers=1,
                         # Add configuration to handle NIC reuse
                         network_interface_name_prefix="dask-nic",
                         # Add delay between worker creation
                         worker_creation_delay=180,
                         # Ensure proper cleanup
                         cleanup_on_close=True)

# Add a small delay before starting the cluster
time.sleep(5)

# Start the cluster
cluster.scale(1)