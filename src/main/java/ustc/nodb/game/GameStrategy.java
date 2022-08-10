package ustc.nodb.game;

public interface GameStrategy {
    public void initGame();
    public void startGame();
    public double computeCost(int clusterId, int partition);
}
