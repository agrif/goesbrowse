"""fix up filters to be more sqlite-friendly

Revision ID: b44376bdacc2
Revises: f6a034af334b
Create Date: 2020-11-19 15:39:02.701397

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'b44376bdacc2'
down_revision = 'f6a034af334b'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    with op.batch_alter_table('file', schema=None) as batch_op:
        batch_op.create_index('ix_file_product_id_type', ['product_id', 'type'], unique=False)
        batch_op.drop_index('ix_file_size')
        batch_op.drop_index('ix_file_type')

    with op.batch_alter_table('product', schema=None) as batch_op:
        batch_op.create_index('ix_product_filter', ['id', 'type', 'source', 'region', 'channel', 'style', 'nnn'], unique=False)
        batch_op.drop_index('idx_filter')
        batch_op.drop_index('ix_product_channel')
        batch_op.drop_index('ix_product_height')
        batch_op.drop_index('ix_product_name')
        batch_op.drop_index('ix_product_nnn')
        batch_op.drop_index('ix_product_region')
        batch_op.drop_index('ix_product_source')
        batch_op.drop_index('ix_product_style')
        batch_op.drop_index('ix_product_type')
        batch_op.drop_index('ix_product_width')
        batch_op.drop_index('ix_product_xxx')

    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    with op.batch_alter_table('product', schema=None) as batch_op:
        batch_op.create_index('ix_product_xxx', ['xxx'], unique=False)
        batch_op.create_index('ix_product_width', ['width'], unique=False)
        batch_op.create_index('ix_product_type', ['type'], unique=False)
        batch_op.create_index('ix_product_style', ['style'], unique=False)
        batch_op.create_index('ix_product_source', ['source'], unique=False)
        batch_op.create_index('ix_product_region', ['region'], unique=False)
        batch_op.create_index('ix_product_nnn', ['nnn'], unique=False)
        batch_op.create_index('ix_product_name', ['name'], unique=False)
        batch_op.create_index('ix_product_height', ['height'], unique=False)
        batch_op.create_index('ix_product_channel', ['channel'], unique=False)
        batch_op.create_index('idx_filter', ['type', 'source', 'region', 'channel', 'style', 'nnn'], unique=False)
        batch_op.drop_index('ix_product_filter')

    with op.batch_alter_table('file', schema=None) as batch_op:
        batch_op.create_index('ix_file_type', ['type'], unique=False)
        batch_op.create_index('ix_file_size', ['size'], unique=False)
        batch_op.drop_index('ix_file_product_id_type')

    # ### end Alembic commands ###