package com.aliyun.hitsdb.client.value.type;

public enum UserPrivilege {
    WRITE_ONLY((byte)1),     //  001
    READ_ONLY((byte)2),      //  010
    READ_WRITE((byte)3),     //  011
    SUPER((byte)7);          //  111

    private final byte id;

    UserPrivilege(byte id) { this.id = id; }

    public byte id() { return this.id; }

    public static UserPrivilege fromId(int id) {
        switch (id) {
            case 1:
                return WRITE_ONLY;
            case 2:
                return READ_ONLY;
            case 3:
                return READ_WRITE;
            case 7:
                return SUPER;
            default:
                throw new IllegalArgumentException("No privilege match for id [" + id + "]");
        }

    }

    public static UserPrivilege fromString(String privilege) {
        if ("writeonly".equals(privilege)) {
            return WRITE_ONLY;
        } else if ("readonly".equals(privilege)) {
            return READ_ONLY;
        } else if ("readwrite".equals(privilege)) {
            return READ_WRITE;
        } else if ("super".equals(privilege)) {
            return SUPER;
        } else {
            throw new IllegalArgumentException("No privilege match for string [" + privilege + "]");
        }
    }

    public boolean canRead() { return ((this.id & READ_ONLY.id) == READ_ONLY.id); }

    public boolean canWrite() {
        return ((this.id & WRITE_ONLY.id) == WRITE_ONLY.id);
    }

    public boolean isSuper() { return this.id == SUPER.id; }
}
