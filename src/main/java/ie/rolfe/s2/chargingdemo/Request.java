package ie.rolfe.s2.chargingdemo;

class Request {
    public int randomuser;
    public long pid;
    public long txId;

    public Request(long txId, long pid, int randomuser) {
        this.txId = txId;
        this.pid = pid;
        this.randomuser = randomuser;
    }
}
