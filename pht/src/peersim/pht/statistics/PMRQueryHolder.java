package peersim.pht.statistics;

/**
 * Store the needed information about a range query message.
 */
class PMRQueryHolder implements Comparable<PMRQueryHolder> {
    private final long id;
    private final long count;
    private final String keyMin;
    private final String keyMax;

    public PMRQueryHolder(long id, long count, String keyMin, String keyMax) {
        this.id     = id;
        this.count  = count;
        this.keyMin = keyMin;
        this.keyMax = keyMax;
    }

    public long getId() {
        return id;
    }

    public long getCount() {
        return count;
    }

    public String getKeyMin() {
        return keyMin;
    }

    public String getKeyMax() {
        return keyMax;
    }

    /**
     * Sort by count unless the two count fields are equal, in this case
     * sort by the id (which is unique).
     * @param pmrQueryHolder PMRQueryHolder to compare to.
     * @return -1, 0, 1 if this PMRQueryHolder is inferior, equel, superior to
     * pmrQueryHolder.
     */
    @Override
    public int compareTo(PMRQueryHolder pmrQueryHolder) {

        /*
         * Since we return an int and both count and id fields are long,
         * to avoid any potential error due to long to int cast, return
         * constant values: -1, 0 or 1.
         */
        if (this.count < pmrQueryHolder.count) {
            return -1;
        } else if (this.count == pmrQueryHolder.count) {
            if (this.id < pmrQueryHolder.id) {
                return -1;
            } else if (this.id == pmrQueryHolder.id) {
                return 0;
            } else {
                return 1;
            }
        } else {
            return 1;
        }
    }
}
